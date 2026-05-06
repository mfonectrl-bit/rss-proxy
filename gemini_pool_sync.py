"""
GeminiPool Sync v3.1 — Fixed & Improved
- Fix memory leak khi thay đổi số lượng API keys
- Reset cycle đúng cách
- Prune loop an toàn hơn
- Logging rõ ràng
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

RETRY_AFTER_ERROR: float = 60.0
MAX_CONCURRENT_TRANSLATES: int = 4
PRUNE_INTERVAL: int = 45
CYCLE_CACHE_TTL: float = 5.0

# ===================================================================


@dataclass
class SlotState:
    rpm_log:     deque = field(default_factory=deque)
    retry_after: float = 0.0
    last_req:    float = 0.0

    def prune_rpm(self, now: float) -> None:
        cutoff = now - 60.0
        while self.rpm_log and self.rpm_log[0] < cutoff:
            self.rpm_log.popleft()

    def can_use(self, alias: str, now: float, n_keys: int) -> bool:
        if now < self.retry_after:
            return False
        interval = QUOTA[alias]["interval"] / max(n_keys, 1)
        if now - self.last_req < interval:
            return False
        self.prune_rpm(now)
        if len(self.rpm_log) >= QUOTA[alias]["rpm"]:
            return False
        return True

    def record_usage(self, now: float) -> None:
        self.rpm_log.append(now)
        self.last_req = now


class GeminiPool:
    """
    Gemini API Pool v3.1 — Fixed version
    """

    MODELS = GEMINI_MODELS
    PREFIX = GEMINI_PREFIX

    def __init__(self, keys: List[str], system_loaded_fn: Callable[[], bool] = lambda: True):
        self._keys: List[str] = list(keys)
        self._lock = threading.Lock()
        self._sem = threading.Semaphore(MAX_CONCURRENT_TRANSLATES)

        # Khởi tạo slots sạch sẽ
        n_keys = max(len(self._keys), 1)
        self._slots: Dict[Tuple[str, int], SlotState] = {
            (alias, ki): SlotState()
            for alias in QUOTA
            for ki in range(n_keys)
        }

        # Reset cycle
        self._cycle: List[Tuple[str, int]] = []
        self._cycle_expires: float = 0.0
        self._cycle_idx: int = 0

        # Background prune
        threading.Thread(
            target=self._prune_loop, 
            daemon=True, 
            name='GeminiPool-Prune'
        ).start()

        print(
            f'[GeminiPool v3.1] keys={len(self._keys)} | '
            f'models={list(QUOTA.keys())} | '
            f'max_concurrent={MAX_CONCURRENT_TRANSLATES} | '
            f'retry_after_error={RETRY_AFTER_ERROR}s'
        )

    def translate_context(self):
        return self._sem

    def pick_slot(self) -> Optional[Tuple[str, int, str]]:
        if not self._keys:
            return None

        now = time.time()
        n_keys = len(self._keys)

        with self._lock:
            self._refresh_cycle_if_needed(now)
            if not self._cycle:
                return None

            n = len(self._cycle)
            max_attempts = min(n * 2, 200)

            for attempt in range(max_attempts):
                idx = (self._cycle_idx + attempt) % n
                alias, ki = self._cycle[idx]
                slot = self._slots.get((alias, ki))

                if slot and slot.can_use(alias, now, n_keys):
                    slot.record_usage(now)
                    self._cycle_idx = (idx + 1) % n
                    return alias, ki, self._keys[ki]

        return None

    def record_success(self, alias: str, ki: int) -> None:
        with self._lock:
            slot = self._slots.get((alias, ki))
            if slot:
                slot.retry_after = 0.0

    def record_failure(self, alias: str, ki: int, is_rate_limit: bool = False) -> None:
        now = time.time()
        retry_until = now + RETRY_AFTER_ERROR

        with self._lock:
            if is_rate_limit:
                for a in QUOTA:
                    s = self._slots.get((a, ki))
                    if s:
                        s.retry_after = retry_until
                print(f'[GeminiPool] key{ki} 429 → ALL models cooldown {RETRY_AFTER_ERROR:.0f}s')
            else:
                slot = self._slots.get((alias, ki))
                if slot:
                    slot.retry_after = retry_until
                label = f'{alias}[key{ki}]' if len(self._keys) > 1 else alias
                print(f'[GeminiPool] {label} lỗi → retry sau {RETRY_AFTER_ERROR:.0f}s')

            # Reset cycle để thử các slot khác ngay
            self._cycle_idx = 0

    def slot_status(self) -> str:
        now = time.time()
        n_keys = len(self._keys)
        lines = ['[GeminiPool] Slot status:']
        with self._lock:
            for alias in QUOTA:
                for ki in range(n_keys):
                    slot = self._slots.get((alias, ki))
                    if not slot:
                        continue
                    slot.prune_rpm(now)
                    avail = slot.can_use(alias, now, n_keys)
                    remain = max(0.0, slot.retry_after - now)
                    rpm = len(slot.rpm_log)
                    label = f'[key{ki}]' if n_keys > 1 else ''
                    lines.append(
                        f'  {alias}{label}: rpm={rpm}/{QUOTA[alias]["rpm"]} | '
                        f'retry_in={remain:.0f}s | available={avail}'
                    )
        return '\n'.join(lines)

    def metrics(self) -> str:
        # Giữ nguyên hoặc bỏ nếu không dùng
        return "# GeminiPool metrics placeholder\n"

    # ====================== INTERNAL ======================

    def _build_cycle(self) -> List[Tuple[str, int]]:
        cycle: List[Tuple[str, int]] = []
        for alias, weight in BASE_WEIGHTS.items():
            for ki in range(len(self._keys)):
                cycle.extend([(alias, ki)] * weight)
        return cycle

    def _refresh_cycle_if_needed(self, now: float) -> None:
        if now > self._cycle_expires:
            self._cycle = self._build_cycle()
            self._cycle_expires = now + CYCLE_CACHE_TTL

    def _prune_loop(self) -> None:
        """Prune loop an toàn"""
        while True:
            try:
                time.sleep(PRUNE_INTERVAL)
                now = time.time()
                with self._lock:
                    for slot in self._slots.values():
                        slot.prune_rpm(now)
            except Exception as e:
                print(f'[GeminiPool Prune] Lỗi: {e}')
                time.sleep(10)