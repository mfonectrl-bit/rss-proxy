"""
GeminiPool Sync v2.4 — Clean, Optimized & Production Ready
Drop-in replacement cho EngineDispatcher trong proxy5.
"""

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

# ========================== CONFIGURATION ==========================

QUOTA: Dict[str, Dict[str, float]] = {
    "gemini-2.5":      {"rpm": 5,   "rpd": 20,  "interval": 13.0},
    "gemini-2.5-lite": {"rpm": 10,  "rpd": 20,  "interval": 6.5},
    "gemini-3.0":      {"rpm": 5,   "rpd": 20,  "interval": 13.0},
    "gemini-3.1":      {"rpm": 15,  "rpd": 500, "interval": 4.5},
}

GEMINI_MODELS: Dict[str, str] = {
    "gemini-2.5":      "gemini-2.5-flash",
    "gemini-2.5-lite": "gemini-2.5-flash-lite-preview",
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

# Fallback settings
FALLBACK_MODELS: List[str] = ["gemini-2.5-lite", "gemini-3.1", "gemini-2.5"]
FALLBACK_RPM_MULTIPLIER: float = 0.6
FALLBACK_COOLDOWN: float = 15.0

# Health & Cooldown
HEALTH_DECAY_PER_MIN: float = 1.5
HEALTH_RECOVERY: float = 12.0
HEALTH_PENALTY: float = 22.0
MIN_HEALTH_FOR_USE: float = 25.0

COOLDOWN_BASE: int = 30
COOLDOWN_CAP: int = 600
MAX_FAILS: int = 2

MAX_CONCURRENT_TRANSLATES: int = 4
PRUNE_INTERVAL: int = 45
CYCLE_CACHE_TTL: float = 5.0

# ===================================================================


@dataclass
class KeyHealth:
    """Health tracking cho từng (model, key)"""
    score: float = 100.0
    error_streak: int = 0
    last_success: float = 0.0
    last_error: float = 0.0
    last_decay: float = field(default_factory=time.time)

    def decay(self, now: float) -> None:
        minutes = (now - self.last_decay) / 60.0
        if minutes > 0.1:
            self.score = max(10.0, self.score - minutes * HEALTH_DECAY_PER_MIN)
            self.last_decay = now

    def on_success(self) -> None:
        self.decay(time.time())
        self.score = min(100.0, self.score + HEALTH_RECOVERY)
        self.error_streak = 0
        self.last_success = time.time()

    def on_failure(self) -> None:
        self.decay(time.time())
        self.score = max(10.0, self.score - HEALTH_PENALTY)
        self.error_streak += 1
        self.last_error = time.time()


@dataclass
class SlotState:
    """State của một slot (model_alias, key_index)"""
    rpm_log: deque[float] = field(default_factory=deque)
    rpd_log: deque[float] = field(default_factory=deque)
    fails: int = 0
    cooldown_until: float = 0.0
    last_req: float = 0.0
    health: KeyHealth = field(default_factory=KeyHealth)
    is_fallback: bool = False

    def prune(self, now: float) -> None:
        cutoff_rpm = now - 60.0
        cutoff_rpd = now - 86400.0
        while self.rpm_log and self.rpm_log[0] < cutoff_rpm:
            self.rpm_log.popleft()
        while self.rpd_log and self.rpd_log[0] < cutoff_rpd:
            self.rpd_log.popleft()

    def can_use(self, alias: str, now: float, n_keys: int, is_fallback: bool = False) -> bool:
        if self.health.score < MIN_HEALTH_FOR_USE:
            return False
        if now < self.cooldown_until:
            return False

        interval = QUOTA[alias]["interval"] / max(n_keys, 1)
        if now - self.last_req < interval:
            return False

        rpm_limit = QUOTA[alias]["rpm"]
        if is_fallback:
            rpm_limit = max(1, int(rpm_limit * FALLBACK_RPM_MULTIPLIER))

        if len(self.rpm_log) >= rpm_limit:
            return False
        if len(self.rpd_log) >= QUOTA[alias]["rpd"]:
            return False
        return True

    def record_usage(self, now: float) -> None:
        self.rpm_log.append(now)
        self.rpd_log.append(now)
        self.last_req = now


class GeminiPool:
    """Gemini API Pool với adaptive weight, fallback thông minh và chống burst"""

    MODELS = GEMINI_MODELS
    PREFIX = GEMINI_PREFIX

    def __init__(self, keys: List[str], system_loaded_fn: Callable[[], bool] = lambda: True):
        self._keys: List[str] = list(keys)
        self._is_loaded = system_loaded_fn
        self._lock = threading.Lock()
        self._sem = threading.Semaphore(MAX_CONCURRENT_TRANSLATES)

        # Initialize slots
        self._slots: Dict[Tuple[str, int], SlotState] = {}
        n_keys = max(len(self._keys), 1)
        for ki in range(n_keys):
            for alias in QUOTA:
                self._slots[(alias, ki)] = SlotState()

        # Cycle cache
        self._cycle: List[Tuple[str, int]] = []
        self._cycle_expires: float = 0.0
        self._cycle_idx: int = 0

        # Fallback tracking
        self._last_fallback_time: float = 0.0
        self._fallback_count: int = 0

        # Background prune thread
        self._prune_thread = threading.Thread(
            target=self._prune_loop, daemon=True, name="GeminiPool-Prune"
        )
        self._prune_thread.start()

        self._log_startup()

    # ====================== PUBLIC API ======================

    def translate_context(self):
        """Context manager chống burst 429"""
        return self._sem

    def pick_slot(self) -> Optional[Tuple[str, int, str]]:
        """Trả về (model_alias, key_index, api_key) hoặc None"""
        if not self._keys or not self._is_loaded():
            return None

        now = time.time()
        n_keys = len(self._keys)

        with self._lock:
            self._refresh_cycle_if_needed(now)

            # Thử primary cycle trước
            result = self._try_pick_primary(now, n_keys)
            if result:
                return result

            # Nếu không được thì fallback
            return self._try_pick_fallback(now, n_keys)

    def record_success(self, alias: str, ki: int) -> None:
        with self._lock:
            slot = self._slots.get((alias, ki))
            if slot:
                slot.fails = 0
                slot.health.on_success()
                slot.is_fallback = False

    def record_failure(self, alias: str, ki: int, is_rate_limit: bool = False) -> None:
        with self._lock:
            slot = self._slots.get((alias, ki))
            if not slot:
                return

            slot.health.on_failure()
            slot.fails += 1

            if is_rate_limit or slot.fails >= MAX_FAILS:
                cooldown = min(COOLDOWN_BASE * (2 ** (slot.fails - 1)), COOLDOWN_CAP)
                if is_rate_limit:
                    cooldown = int(cooldown * 1.8)

                now = time.time()
                reason = "rate_limit" if is_rate_limit else "consecutive fails"

                if is_rate_limit:
                    # 429 là per-API-key → cooldown TẤT CẢ models dùng cùng key này
                    for a in QUOTA:
                        s = self._slots.get((a, ki))
                        if s:
                            s.cooldown_until = now + cooldown
                            s.fails = 0
                    print(f"[GeminiPool] key{ki} → ALL models cooldown {cooldown}s ({reason})")
                else:
                    slot.cooldown_until = now + cooldown
                    slot.fails = 0
                    label = f"{alias}[key{ki}]" if len(self._keys) > 1 else alias
                    fb = " (fallback)" if slot.is_fallback else ""
                    print(f"[GeminiPool] {label}{fb} → cooldown {cooldown}s ({reason})")

    def metrics(self) -> str:
        """Prometheus metrics"""
        now = time.time()
        lines: List[str] = [
            "# HELP gemini_pool_rpm_current Current RPM usage",
            "# TYPE gemini_pool_rpm_current gauge",
        ]

        with self._lock:
            for alias in QUOTA:
                for ki in range(len(self._keys)):
                    slot = self._slots[(alias, ki)]
                    label = f'model="{alias}",key="{ki}"'
                    lines.append(f'gemini_pool_rpm_current{{{label}}} {len(slot.rpm_log)}')
                    lines.append(f'gemini_pool_rpd_current{{{label}}} {len(slot.rpd_log)}')
                    lines.append(f'gemini_pool_health_score{{{label}}} {slot.health.score:.1f}')
                    lines.append(f'gemini_pool_cooldown_remaining{{{label}}} {max(0.0, slot.cooldown_until - now):.1f}')
                    lines.append(f'gemini_pool_available{{{label}}} {int(slot.can_use(alias, now, len(self._keys)))}')

            lines.append(f"gemini_pool_fallback_total {self._fallback_count}")

        return "\n".join(lines) + "\n"

    # ====================== INTERNAL ======================

    def _get_effective_weight(self, alias: str, ki: int, now: float) -> float:
        slot = self._slots[(alias, ki)]
        slot.health.decay(now)
        health_factor = max(0.3, slot.health.score / 100.0)
        return BASE_WEIGHTS.get(alias, 1) * health_factor

    def _build_dynamic_cycle(self, now: float) -> List[Tuple[str, int]]:
        if not self._keys:
            return []
        cycle: List[Tuple[str, int]] = []
        n = len(self._keys)

        for alias in BASE_WEIGHTS:
            for ki in range(n):
                weight = self._get_effective_weight(alias, ki, now)
                repeats = max(1, round(weight))
                cycle.extend([(alias, ki)] * repeats)
        return cycle

    def _refresh_cycle_if_needed(self, now: float) -> None:
        if now > self._cycle_expires:
            self._cycle = self._build_dynamic_cycle(now)
            self._cycle_expires = now + CYCLE_CACHE_TTL

    def _try_pick_primary(self, now: float, n_keys: int) -> Optional[Tuple[str, int, str]]:
        if not self._cycle:
            return None

        n = len(self._cycle)
        max_attempts = min(n * 2, 120)

        for attempt in range(max_attempts):
            idx = (self._cycle_idx + attempt) % n
            alias, ki = self._cycle[idx]
            slot = self._slots[(alias, ki)]

            if slot.can_use(alias, now, n_keys, is_fallback=False):
                slot.record_usage(now)
                self._cycle_idx = (idx + 1) % n
                return alias, ki, self._keys[ki]

        return None

    def _try_pick_fallback(self, now: float, n_keys: int) -> Optional[Tuple[str, int, str]]:
        if now - self._last_fallback_time < FALLBACK_COOLDOWN:
            return None

        for alias in FALLBACK_MODELS:
            for ki in range(n_keys):
                slot = self._slots.get((alias, ki))
                if slot and slot.can_use(alias, now, n_keys, is_fallback=True):
                    slot.record_usage(now)
                    slot.is_fallback = True
                    self._last_fallback_time = now
                    self._fallback_count += 1

                    label = f"{alias}[key{ki}]"
                    print(f"[GeminiPool] FALLBACK ACTIVATED → {label} (total={self._fallback_count})")
                    return alias, ki, self._keys[ki]

        return None

    def _prune_loop(self) -> None:
        while True:
            time.sleep(PRUNE_INTERVAL)
            now = time.time()
            with self._lock:
                for slot in self._slots.values():
                    slot.prune(now)
                    slot.health.decay(now)

    def _log_startup(self) -> None:
        print(
            f"[GeminiPool v2.4] Initialized | "
            f"keys={len(self._keys)} | "
            f"max_concurrent={MAX_CONCURRENT_TRANSLATES} | "
            f"fallback=enabled | cycle_cache={CYCLE_CACHE_TTL}s"
        )