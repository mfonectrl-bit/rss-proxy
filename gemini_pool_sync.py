import time
import random
import threading

class GeminiPool:
    def __init__(self, clients, quota_per_key=60, reset_time=60):
        """
        clients: list API keys hoặc client objects
        quota_per_key: số request tối đa / key / window
        reset_time: thời gian reset quota (giây)
        """
        self.clients = clients
        self.quota_per_key = quota_per_key
        self.reset_time = reset_time

        self.lock = threading.Lock()
        self.usage = {c: 0 for c in clients}
        self.last_reset = {c: time.time() for c in clients}

    def _reset_if_needed(self, client):
        now = time.time()
        if now - self.last_reset[client] >= self.reset_time:
            self.usage[client] = 0
            self.last_reset[client] = now

    def _get_available_client(self):
        with self.lock:
            random.shuffle(self.clients)

            for client in self.clients:
                self._reset_if_needed(client)

                if self.usage[client] < self.quota_per_key:
                    self.usage[client] += 1
                    return client

        return None

    def request(self, func, *args, retries=3, **kwargs):
        """
        func: function gọi API, phải nhận client làm param đầu tiên
        retries: số lần retry nếu fail
        """
        for attempt in range(retries):
            client = self._get_available_client()

            if not client:
                sleep_time = random.uniform(0.5, 1.5)
                time.sleep(sleep_time)
                continue

            try:
                return func(client, *args, **kwargs)

            except Exception as e:
                # giảm usage nếu fail để không phí quota
                with self.lock:
                    self.usage[client] = max(0, self.usage[client] - 1)

                # jitter retry
                time.sleep(random.uniform(0.5, 2))

        raise Exception("All retries failed")