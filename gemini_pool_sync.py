"""
GeminiPool Sync v1.1 — Drop-in replacement cho EngineDispatcher trong proxy5.

Ported từ gemini_pool_v4 (async) sang sync threading để tương thích
với proxy5's ThreadPoolExecutor architecture.

Cải tiến so với EngineDispatcher cũ:
  - deque-based RPM/RPD tracking (O(1) prune, không list comprehension inline)
  - RPD enforce thực sự cho gemini-3.0 (20 RPD/key)
  - KeyHealth score thay thế fail counter đơn giản
  - Prune tách khỏi check — không side effect ngầm
  - pick_slot() atomic trong lock, không double-pick
  - MAX_FAILS = 2 (ít nhạy cảm hơn với timeout mạng thoáng qua)
  - Fix gemini-3.0 missing trong _is_available()
  - Weighted cycle giữ nguyên từ proxy5 (GM3.1×3, GM2.5L×2, ...)
  - Semaphore burst guard: giới hạn MAX_CONCURRENT_TRANSLATES đồng thời
    → chặn 429 tức thời do burst sau restart/flood real-time feeds

Interface giữ nguyên 100% để không đổi caller:
  dispatcher = GeminiPool(keys, system_loaded_fn)
  slot = dispatcher.pick_slot()               # (model_alias, ki, api_key) | None
  dispatcher.record_success(model, ki)
  dispatcher.record_failure(model, ki, is_rate_limit=bool)
  dispatcher.status()                          # dict cho /stats endpoint
  dispatcher.translate_context()               # context manager cho semaphore

Không import google-genai — dùng urllib.request như proxy5.
"""

import math
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

# ──────────────────────────────────────────────
#  CONFIG — quota free tier tháng 4/2026
# ──────────────────────────────────────────────
QUOTA: Dict[str, Dict] = {
    # alias          rpm   rpd    min_interval(s)
    "gemini-2.5":     {"rpm": 5,  "rpd": 250,  "interval": 13.0},
    "gemini-2.5-lite":{"rpm": 10, "rpd": 1000, "interval": 6.5},
    "gemini-3.0":     {"rpm": 5,  "rpd": 20,   "interval": 13.0},  # RPD=20 rất thấp!
    "gemini-3.1":     {"rpm": 15, "rpd": 1500, "interval": 4.5},
}

# Tên model thực gửi lên API
GEMINI_MODELS: Dict[str, str] = {
    "gemini-2.5":      "gemini-2.5-flash",
    "gemini-2.5-lite": "gemini-2.5-flash-lite-preview",  # fix: thêm -preview
    "gemini-3.0":      "gemini-3-flash-preview",
    "gemini-3.1":      "gemini-3.1-flash-lite-preview",
}

GEMINI_PREFIX: Dict[str, str] = {
    "gemini-2.5":      "GM2.5",
    "gemini-2.5-lite": "GM2.5L",
    "gemini-3.0":      "GM3.0",
    "gemini-3.1":      "GM3.1",
}

# Weighted base cycle: tỉ lệ tương ứng RPM
# GM3.1(15)×3 : GM2.5L(10)×2 : GM2.5(5)×1 : GM3.0(5)×1
# GM3.0 chỉ 1 slot vì RPD=20/key rất thấp
_BASE_CYCLE: List[str] = [
    "gemini-3.1", "gemini-2.5-lite",
    "gemini-3.1", "gemini-2.5",
    "gemini-3.1", "gemini-2.5-lite",
    "gemini-3.0", "gemini-2.5-lite",
]

# Cooldown params
COOLDOWN_BASE = 30      # giây, cooldown đầu tiên
COOLDOWN_CAP  = 480     # giây, tối đa
MAX_FAILS     = 2       # lỗi liên tiếp trước khi vào cooldown (tăng từ 1 → 2)

# Prune background interval
PRUNE_INTERVAL = 60     # giây

# Burst guard: số translate Gemini được chạy đồng thời tối đa.
# Dù RPM tracking chặn theo phút, nhiều thread vẫn có thể pick slot
# hợp lệ cùng lúc và gửi burst → Google trả 429 tức thời dù quota còn.
# Công thức an toàn: min_rpm × n_keys / 2 = 5 × 5 / 2 = 12 → chọn 4 conservative.
MAX_CONCURRENT_TRANSLATES = 4

# Token bucket: khoảng cách tối thiểu giữa 2 request Gemini bất kỳ (giây).
# Google có per-second burst limit không documented — 4 req/s đã đủ trigger 429
# dù RPM còn đầy. Với 1.5s interval: tối đa ~40 req/phút tổng cộng,
# phân bổ đều → không bao giờ burst dù 106 channels flood cùng lúc.
MIN_REQUEST_INTERVAL = 1.5  # giây


# ──────────────────────────────────────────────
#  DATA STRUCTURES
# ──────────────────────────────────────────────
@dataclass
class KeyHealth:
    """Health score per (model_alias, key_index)."""
    score: float = 100.0     # 0–100, càng cao càng được ưu tiên
    error_streak: int = 0
    last_success: float = 0.0
    last_error: float = 0.0

    def on_success(self) -> None:
        self.score = min(100.0, self.score + 10.0)
        self.error_streak = 0
        self.last_success = time.time()

    def on_failure(self) -> None:
        self.score = max(5.0, self.score - 18.0)
        self.error_streak += 1
        self.last_error = time.time()


@dataclass
class SlotState:
    """Rate-limit tracking cho 1 (model_alias, key_index) slot."""
    rpm_log: deque = field(default_factory=deque)   # timestamps trong 60s
    rpd_log: deque = field(default_factory=deque)   # timestamps trong 24h
    fails: int = 0
    cooldown_until: float = 0.0
    last_req: float = 0.0
    health: KeyHealth = field(default_factory=KeyHealth)

    def prune(self, now: float) -> None:
        """Prune stale entries — gọi tường minh, không làm trong check."""
        cutoff_rpm = now - 60.0
        cutoff_rpd = now - 86400.0
        while self.rpm_log and self.rpm_log[0] < cutoff_rpm:
            self.rpm_log.popleft()
        while self.rpd_log and self.rpd_log[0] < cutoff_rpd:
            self.rpd_log.popleft()

    def record(self, now: float) -> None:
        self.rpm_log.append(now)
        self.rpd_log.append(now)
        self.last_req = now


# ──────────────────────────────────────────────
#  MAIN CLASS
# ──────────────────────────────────────────────
class GeminiPool:
    """
    Sync weighted round-robin pool cho Gemini multi-key.

    Usage trong proxy5:
        _gemini_pool = GeminiPool(
            keys=GEMINI_API_KEYS,
            system_loaded_fn=lambda: _system_fully_loaded,
        )

        # pick slot (trong lock nếu cần, pool tự lock nội bộ)
        slot = _gemini_pool.pick_slot()
        if slot:
            model_alias, ki, api_key = slot
            model_name = GeminiPool.MODELS[model_alias]
            result = _translate_gemini(text, model=model_name, api_key=api_key)
            _gemini_pool.record_success(model_alias, ki)
    """

    MODELS  = GEMINI_MODELS
    PREFIX  = GEMINI_PREFIX

    def __init__(
        self,
        keys: List[str],
        system_loaded_fn: Callable[[], bool] = lambda: True,
    ):
        self._keys = list(keys)
        self._is_loaded = system_loaded_fn
        self._lock = threading.Lock()

        # Burst guard semaphore — giới hạn số translate đồng thời
        # Chặn 429 tức thời do nhiều thread pick slot cùng lúc sau restart/flood
        self._sem = threading.Semaphore(MAX_CONCURRENT_TRANSLATES)

        # Token bucket — enforce khoảng cách tối thiểu giữa các request
        # Dùng lock riêng để không block pick_slot() trong _lock
        self._rate_lock = threading.Lock()
        self._last_request_time: float = 0.0

        # Build cycle + state
        self._cycle: List[Tuple[str, int]] = self._build_cycle()
        self._cycle_idx: int = 0
        self._slots: Dict[Tuple[str, int], SlotState] = {}

        n = max(len(self._keys), 1)
        for ki in range(n):
            for alias in QUOTA:
                self._slots[(alias, ki)] = SlotState()

        # Prune background thread
        self._prune_thread = threading.Thread(
            target=self._prune_loop, daemon=True, name="GeminiPool-Prune"
        )
        self._prune_thread.start()

        # Log khởi động
        from collections import Counter
        dist = Counter(ki for _, ki in self._cycle)
        dist_str = ", ".join(f"key{k}:{v}" for k, v in sorted(dist.items()))
        print(
            f"[GeminiPool] Init: {len(self._keys)} keys | "
            f"cycle={len(self._cycle)} slots | {dist_str} | "
            f"max_concurrent={MAX_CONCURRENT_TRANSLATES} | "
            f"min_interval={MIN_REQUEST_INTERVAL}s"
        )

    # ── burst guard context manager ────────────
    def translate_context(self):
        """
        Context manager bao quanh toàn bộ 1 lần translate (bao gồm cả HTTP call).
        Kết hợp 2 lớp bảo vệ:
          1. Semaphore: giới hạn MAX_CONCURRENT_TRANSLATES thread đồng thời
          2. Token bucket: enforce MIN_REQUEST_INTERVAL giữa các request
             → chặn per-second burst limit của Google (undocumented)
        """
        return self._sem

    def wait_rate_limit(self) -> None:
        """
        Token bucket — gọi TRƯỚC khi gửi HTTP request thực sự.
        Block thread hiện tại cho đến khi đủ MIN_REQUEST_INTERVAL
        kể từ request gần nhất. Thread-safe.
        """
        with self._rate_lock:
            now = time.time()
            elapsed = now - self._last_request_time
            wait = MIN_REQUEST_INTERVAL - elapsed
            if wait > 0:
                time.sleep(wait)
            self._last_request_time = time.time()

    # ── cycle builder ──────────────────────────
    def _build_cycle(self) -> List[Tuple[str, int]]:
        """
        Interleave _BASE_CYCLE × keys bằng LCM để mỗi key
        xuất hiện đúng tỉ lệ trong cycle.
        """
        n = len(self._keys)
        if n == 0:
            return []
        base_len = len(_BASE_CYCLE)
        repeat = math.lcm(base_len, n) // base_len
        cycle: List[Tuple[str, int]] = []
        for rep in range(repeat):
            for i, alias in enumerate(_BASE_CYCLE):
                global_i = rep * base_len + i
                cycle.append((alias, global_i % n))
        # Đảm bảo mỗi key xuất hiện ít nhất 1 lần
        covered = {ki for _, ki in cycle}
        for ki in range(n):
            if ki not in covered:
                cycle.append(("gemini-3.1", ki))
        return cycle

    # ── availability check (pure, no side effect) ──
    def _is_available(self, alias: str, ki: int, now: float) -> bool:
        """
        Kiểm tra slot có sẵn sàng không.
        KHÔNG prune, KHÔNG ghi state — chỉ đọc.
        Prune đã được tách ra _prune_loop() và được gọi tường minh.
        """
        if not self._is_loaded():
            return False
        if ki >= len(self._keys):
            return False

        slot = self._slots.get((alias, ki))
        if slot is None:
            return False

        # Health score quá thấp → skip
        if slot.health.score < 25.0:
            return False

        # Đang cooldown
        if now < slot.cooldown_until:
            return False

        # Min interval giữa 2 request liên tiếp trên cùng slot
        # Chia theo số keys vì quota là per-project
        n = max(len(self._keys), 1)
        interval = QUOTA[alias]["interval"] / n
        if interval > 0 and (now - slot.last_req) < interval:
            return False

        # RPM check
        if len(slot.rpm_log) >= QUOTA[alias]["rpm"]:
            return False

        # RPD check — enforce thực sự (fix cho gemini-3.0 RPD=20)
        if len(slot.rpd_log) >= QUOTA[alias]["rpd"]:
            return False

        return True

    # ── pick (atomic, thread-safe) ─────────────
    def pick_slot(self) -> Optional[Tuple[str, int, str]]:
        """
        Pick (model_alias, key_index, api_key) theo weighted round-robin.
        Atomic: check + record trong cùng 1 lock acquisition.
        Trả về None nếu tất cả slots exhausted.
        """
        if not self._keys or not self._is_loaded() or not self._cycle:
            return None

        now = time.time()
        n = len(self._cycle)

        with self._lock:
            start = self._cycle_idx
            tried: set = set()

            for attempt in range(n):
                idx = (start + attempt) % n
                alias, ki = self._cycle[idx]
                slot_key = (alias, ki)

                if slot_key in tried:
                    continue
                tried.add(slot_key)

                if self._is_available(alias, ki, now):
                    # Advance index để lần sau bắt đầu từ slot tiếp theo
                    self._cycle_idx = (idx + 1) % n
                    # Record ngay trong lock — atomic, không race condition
                    self._slots[slot_key].record(now)
                    return alias, ki, self._keys[ki]

        return None

    # ── record outcome ─────────────────────────
    def record_success(self, alias: str, ki: int) -> None:
        with self._lock:
            slot = self._slots.get((alias, ki))
            if slot:
                slot.fails = 0
                slot.health.on_success()

    def record_failure(
        self, alias: str, ki: int, is_rate_limit: bool = False
    ) -> None:
        with self._lock:
            slot = self._slots.get((alias, ki))
            if not slot:
                return

            slot.health.on_failure()
            slot.fails += 1

            should_cooldown = is_rate_limit or slot.fails >= MAX_FAILS
            if should_cooldown:
                cooldown = min(
                    COOLDOWN_BASE * (2 ** (slot.fails - 1)),
                    COOLDOWN_CAP,
                )
                slot.cooldown_until = time.time() + cooldown
                reason = "rate limit" if is_rate_limit else f"{slot.fails} lỗi liên tiếp"
                label = f"{alias}[key{ki}]" if len(self._keys) > 1 else alias
                print(f"[GeminiPool] {label} → cooldown {cooldown}s ({reason})")
                slot.fails = 0  # reset sau khi đã tính cooldown

    # ── prune background ───────────────────────
    def _prune_loop(self) -> None:
        """Prune stale rpm/rpd entries định kỳ để tiết kiệm memory."""
        while True:
            time.sleep(PRUNE_INTERVAL)
            now = time.time()
            with self._lock:
                for slot in self._slots.values():
                    slot.prune(now)

    # ── status dict cho /stats endpoint ────────
    def status(self) -> Dict:
        now = time.time()
        out = {}
        with self._lock:
            for alias in QUOTA:
                for ki in range(len(self._keys)):
                    slot = self._slots.get((alias, ki))
                    if not slot:
                        continue
                    label = f"{alias}[key{ki}]" if len(self._keys) > 1 else alias
                    out[label] = {
                        "rpm":           len(slot.rpm_log),
                        "rpm_limit":     QUOTA[alias]["rpm"],
                        "rpd":           len(slot.rpd_log),
                        "rpd_limit":     QUOTA[alias]["rpd"],
                        "health":        round(slot.health.score, 1),
                        "fails":         slot.fails,
                        "cooldown_left": round(max(0.0, slot.cooldown_until - now)),
                        "available":     self._is_available(alias, ki, now),
                    }
        return out
