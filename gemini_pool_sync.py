"""
GeminiPool Sync v3.0 — Simplified & Reliable
- Bỏ RPD tracking (AI Studio RPD không hoạt động như tài liệu)
- Bỏ health score / decay (gây slot chết ngầm sau ~50m idle)
- Bỏ fallback mechanism (không cần, primary cycle cover hết)
- Chỉ giữ RPM tracking + 60s retry sau mỗi lần lỗi
- Slot bị lỗi → retry_after = now + 60s → tự available lại, không cần probe
"""

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

# ========================== CONFIGURATION ==========================

QUOTA: Dict[str, Dict[str, float]] = {
    "gemini-2.5":      {"rpm": 5,  "interval": 13.0},
    "gemini-2.5-lite": {"rpm": 10, "interval": 6.5},
    "gemini-3.0":      {"rpm": 5,  "interval": 13.0},
    "gemini-3.1":      {"rpm": 15, "interval": 4.5},
}

GEMINI_MODELS: Dict[str, str] = {
    "gemini-2.5":      "gemini-2.5-flash",
    "gemini-2.5-lite": "gemini-2.5-flash-lite",
    "gemini-3.0":      "gemini-3-flash-preview",
    "gemini-3.1":      "gemini-3.1-flash-lite-preview",
}

GEMINI_PREFIX: Dict[str, str] = {
    "gemini-2.5":      "GM2.5",
    "gemini-2.5-lite": "GM2.5L",
    "gemini-3.0":      "GM3.0",
    "gemini-3.1":      "GM3.1",
}

BASE_WEIGHTS: Dict[str, int] = {
    "gemini-3.1":      3,
    "gemini-2.5-lite": 2,
    "gemini-2.5":      1,
    "gemini-3.0":      1,
}

# Sau mỗi lần lỗi, slot tự retry sau bao nhiêu giây
RETRY_AFTER_ERROR: float = 60.0

# Số concurrent dịch tối đa — tránh burst 429
MAX_CONCURRENT_TRANSLATES: int = 4

# RPM log prune mỗi N giây
PRUNE_INTERVAL: int = 45

# Cache cycle trong N giây trước khi rebuild
CYCLE_CACHE_TTL: float = 5.0

# ===================================================================


@dataclass
class SlotState:
    """State tối giản của 1 slot (model_alias, key_index)"""
    rpm_log:     deque = field(default_factory=deque)  # timestamps trong 60s gần nhất
    retry_after: float = 0.0                            # thời điểm slot available trở lại
    last_req:    float = 0.0                            # timestamp request cuối

    def prune_rpm(self, now: float) -> None:
        cutoff = now - 60.0
        while self.rpm_log and self.rpm_log[0] < cutoff:
            self.rpm_log.popleft()

    def can_use(self, alias: str, now: float, n_keys: int) -> bool:
        # Đang trong thời gian retry wait?
        if now < self.retry_after:
            return False
        # Chưa đủ interval giữa 2 request liên tiếp?
        interval = QUOTA[alias]["interval"] / max(n_keys, 1)
        if now - self.last_req < interval:
            return False
        # Vượt RPM?
        self.prune_rpm(now)
        if len(self.rpm_log) >= QUOTA[alias]["rpm"]:
            return False
        return True

    def record_usage(self, now: float) -> None:
        self.rpm_log.append(now)
        self.last_req = now


class GeminiPool:
    """
    Gemini API Pool v3.0 — weighted round-robin, RPM-only, 60s auto-retry.
    Không health decay, không RPD, không fallback riêng — đơn giản và đáng tin.
    """

    MODELS = GEMINI_MODELS
    PREFIX = GEMINI_PREFIX

    def __init__(self, keys: List[str], system_loaded_fn: Callable[[], bool] = lambda: True):
        self._keys:      List[str] = list(keys)
        self._lock       = threading.Lock()
        self._sem        = threading.Semaphore(MAX_CONCURRENT_TRANSLATES)

        # Khởi tạo slot cho mọi (alias, key_index)
        n_keys = max(len(self._keys), 1)
        self._slots: Dict[Tuple[str, int], SlotState] = {
            (alias, ki): SlotState()
            for alias in QUOTA
            for ki in range(n_keys)
        }

        # Weighted cycle cache
        self._cycle:         List[Tuple[str, int]] = []
        self._cycle_expires: float = 0.0
        self._cycle_idx:     int   = 0

        # Background prune thread
        threading.Thread(
            target=self._prune_loop, daemon=True, name='GeminiPool-Prune'
        ).start()

        print(
            f'[GeminiPool v3.0] keys={len(self._keys)} | '
            f'models={list(QUOTA.keys())} | '
            f'max_concurrent={MAX_CONCURRENT_TRANSLATES} | '
            f'retry_after_error={RETRY_AFTER_ERROR}s'
        )

    # ====================== PUBLIC API ======================

    def translate_context(self):
        """Semaphore context để chống burst concurrent"""
        return self._sem

    def pick_slot(self) -> Optional[Tuple[str, int, str]]:
        """
        Trả về (model_alias, key_index, api_key) hoặc None nếu không có slot khả dụng.
        Round-robin theo weighted cycle, skip slot đang trong retry_after.
        """
        if not self._keys:
            return None

        now    = time.time()
        n_keys = len(self._keys)

        with self._lock:
            self._refresh_cycle_if_needed(now)
            if not self._cycle:
                return None

            n            = len(self._cycle)
            max_attempts = min(n * 2, 200)

            for attempt in range(max_attempts):
                idx       = (self._cycle_idx + attempt) % n
                alias, ki = self._cycle[idx]
                slot      = self._slots[(alias, ki)]

                if slot.can_use(alias, now, n_keys):
                    slot.record_usage(now)
                    self._cycle_idx = (idx + 1) % n
                    return alias, ki, self._keys[ki]

        return None

    def record_success(self, alias: str, ki: int) -> None:
        """Gọi sau khi request thành công — reset retry_after"""
        with self._lock:
            slot = self._slots.get((alias, ki))
            if slot:
                slot.retry_after = 0.0

    def record_failure(self, alias: str, ki: int, is_rate_limit: bool = False) -> None:
        """
        Gọi sau khi request thất bại.
        - 429 (rate limit): cooldown TẤT CẢ models trên cùng key (per-key quota)
        - Lỗi khác (503, timeout...): chỉ cooldown slot đó
        Sau RETRY_AFTER_ERROR giây, slot tự available lại — không cần probe.
        """
        now         = time.time()
        retry_until = now + RETRY_AFTER_ERROR

        with self._lock:
            if is_rate_limit:
                # 429 là per-API-key → cooldown tất cả models dùng cùng key này
                for a in QUOTA:
                    s = self._slots.get((a, ki))
                    if s:
                        s.retry_after = retry_until
                print(f'[GeminiPool] key{ki} 429 → ALL models retry sau {RETRY_AFTER_ERROR:.0f}s')
            else:
                slot = self._slots.get((alias, ki))
                if slot:
                    slot.retry_after = retry_until
                label = f'{alias}[key{ki}]' if len(self._keys) > 1 else alias
                print(f'[GeminiPool] {label} lỗi → retry sau {RETRY_AFTER_ERROR:.0f}s')

    def slot_status(self) -> str:
        """Log trạng thái tất cả slots — dùng để debug"""
        now    = time.time()
        n_keys = len(self._keys)
        lines  = ['[GeminiPool] Slot status:']
        with self._lock:
            for alias in QUOTA:
                for ki in range(n_keys):
                    slot   = self._slots[(alias, ki)]
                    slot.prune_rpm(now)
                    avail  = slot.can_use(alias, now, n_keys)
                    remain = max(0.0, slot.retry_after - now)
                    rpm    = len(slot.rpm_log)
                    label  = f'[key{ki}]' if n_keys > 1 else ''
                    lines.append(
                        f'  {alias}{label}: rpm={rpm}/{QUOTA[alias]["rpm"]} | '
                        f'retry_in={remain:.0f}s | available={avail}'
                    )
        return '\n'.join(lines)

    def metrics(self) -> str:
        """Prometheus metrics"""
        now    = time.time()
        n_keys = len(self._keys)
        lines  = [
            '# HELP gemini_pool_rpm_current Current RPM usage per slot',
            '# TYPE gemini_pool_rpm_current gauge',
        ]
        with self._lock:
            for alias in QUOTA:
                for ki in range(n_keys):
                    slot  = self._slots[(alias, ki)]
                    label = f'model="{alias}",key="{ki}"'
                    slot.prune_rpm(now)
                    lines.append(f'gemini_pool_rpm_current{{{label}}} {len(slot.rpm_log)}')
                    lines.append(f'gemini_pool_rpm_limit{{{label}}} {QUOTA[alias]["rpm"]}')
                    lines.append(f'gemini_pool_retry_remaining{{{label}}} {max(0.0, slot.retry_after - now):.1f}')
                    lines.append(f'gemini_pool_available{{{label}}} {int(slot.can_use(alias, now, n_keys))}')
        return '\n'.join(lines) + '\n'

    # ====================== INTERNAL ======================

    def _build_cycle(self) -> List[Tuple[str, int]]:
        """Xây weighted cycle: model có weight cao → xuất hiện nhiều lần hơn"""
        cycle: List[Tuple[str, int]] = []
        for alias, weight in BASE_WEIGHTS.items():
            for ki in range(len(self._keys)):
                cycle.extend([(alias, ki)] * weight)
        return cycle

    def _refresh_cycle_if_needed(self, now: float) -> None:
        if now > self._cycle_expires:
            self._cycle         = self._build_cycle()
            self._cycle_expires = now + CYCLE_CACHE_TTL

    def _prune_loop(self) -> None:
        """Background thread: prune rpm_log cũ định kỳ"""
        while True:
            time.sleep(PRUNE_INTERVAL)
            now = time.time()
            with self._lock:
                for slot in self._slots.values():
                    slot.prune_rpm(now)
