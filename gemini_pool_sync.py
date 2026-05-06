import time
import random
import asyncio
from collections import defaultdict, deque


class GeminiPool:
    def __init__(
        self,
        keys,
        models,
        max_concurrency=5,
        qps_limit=10,
        fail_threshold=5,
        base_cooldown=2,
        max_cooldown=60,
    ):
        self.keys = keys
        self.models = models

        # trạng thái key
        self.key_state = {
            k: {
                "cooldown_until": 0,
                "fail_count": 0,
                "success_count": 0,
                "disabled_until": 0,
                "last_used": 0,
            }
            for k in keys
        }

        # model bị block (region)
        self.model_blocked = set()

        # circuit breaker
        self.fail_threshold = fail_threshold

        # cooldown config
        self.base_cooldown = base_cooldown
        self.max_cooldown = max_cooldown

        # rate limit
        self.sem = asyncio.Semaphore(max_concurrency)
        self.qps_limit = qps_limit
        self.req_timestamps = deque()

    # ======================
    # RATE LIMIT (QPS)
    # ======================
    async def _throttle_qps(self):
        now = time.time()

        while self.req_timestamps and now - self.req_timestamps[0] > 1:
            self.req_timestamps.popleft()

        if len(self.req_timestamps) >= self.qps_limit:
            await asyncio.sleep(0.05)
            return await self._throttle_qps()

        self.req_timestamps.append(now)

    # ======================
    # KEY STATE
    # ======================
    def _is_key_available(self, k):
        s = self.key_state[k]
        now = time.time()
        return (
            now >= s["cooldown_until"]
            and now >= s["disabled_until"]
        )

    def _mark_success(self, k):
        s = self.key_state[k]
        s["fail_count"] = 0
        s["success_count"] += 1
        s["last_used"] = time.time()

    def _mark_fail(self, k):
        s = self.key_state[k]
        s["fail_count"] += 1

        # exponential backoff
        delay = min(self.base_cooldown * (2 ** s["fail_count"]), self.max_cooldown)
        s["cooldown_until"] = time.time() + delay

        # circuit breaker
        if s["fail_count"] >= self.fail_threshold:
            s["disabled_until"] = time.time() + 300  # disable 5 phút
            s["fail_count"] = 0  # reset sau khi disable

    # ======================
    # SCORING (QUAN TRỌNG)
    # ======================
    def _score_key(self, k):
        s = self.key_state[k]
        now = time.time()

        success = s["success_count"]
        fail = s["fail_count"]

        cooldown_penalty = max(0, s["cooldown_until"] - now)
        disabled_penalty = max(0, s["disabled_until"] - now)

        # score càng cao càng ưu tiên
        return (
            success * 2
            - fail * 3
            - cooldown_penalty * 0.1
            - disabled_penalty * 1
            - (now - s["last_used"]) * 0.01  # tránh spam 1 key
        )

    def _get_slots(self):
        slots = []

        for m in self.models:
            if m in self.model_blocked:
                continue

            for k in self.keys:
                if self._is_key_available(k):
                    score = self._score_key(k)
                    slots.append((score, m, k))

        # sort theo score cao → thấp
        slots.sort(reverse=True, key=lambda x: x[0])

        return [(m, k) for _, m, k in slots]

    # ======================
    # MAIN REQUEST
    # ======================
    async def request(self, call_fn, timeout=6, min_available_ratio=0.3):
        slots = self._get_slots()

        # 🚨 FAST FAIL
        available_keys = sum(1 for k in self.keys if self._is_key_available(k))
        if available_keys / len(self.keys) < min_available_ratio:
            raise RuntimeError("Too few available keys → fallback early")

        if not slots:
            raise RuntimeError("No available Gemini slots")

        for model, key in slots:
            try:
                async with self.sem:
                    await self._throttle_qps()

                    result = await asyncio.wait_for(
                        call_fn(model, key),
                        timeout=timeout
                    )

                self._mark_success(key)
                return result

            except Exception as e:
                msg = str(e).lower()

                # 🚫 REGION BLOCK
                if "block region" in msg:
                    self.model_blocked.add(model)
                    continue

                # 🚫 RATE LIMIT
                if "429" in msg or "rate limit" in msg:
                    self._mark_fail(key)
                    continue

                # ❌ BAD REQUEST
                if "400" in msg:
                    self._mark_fail(key)
                    continue

                # timeout
                if isinstance(e, asyncio.TimeoutError):
                    self._mark_fail(key)
                    continue

                # lỗi khác
                self._mark_fail(key)
                continue

        raise RuntimeError("All Gemini slots failed")