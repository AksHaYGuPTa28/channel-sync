from __future__ import annotations

import asyncio
import random
import time


class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int):
        self.rate = rate_per_sec
        self.capacity = burst
        self.tokens = float(burst)
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                await asyncio.sleep((1 - self.tokens) / self.rate)


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, cooldown_sec: float = 2.0):
        self.threshold = failure_threshold
        self.cooldown = cooldown_sec
        self.failures = 0
        self.opened_at: float | None = None
        self.times_opened = 0

    @property
    def state(self) -> str:
        if self.opened_at is None:
            return "closed"
        if time.monotonic() - self.opened_at >= self.cooldown:
            return "half_open"
        return "open"

    def allow(self) -> bool:
        return self.state != "open"

    def retry_in(self) -> float:
        if self.opened_at is None:
            return 0.0
        return max(0.0, self.cooldown - (time.monotonic() - self.opened_at))

    def record_success(self) -> None:
        self.failures = 0
        self.opened_at = None

    def record_failure(self) -> None:
        self.failures += 1
        half_open = self.state == "half_open"
        if half_open or self.failures >= self.threshold:
            # a failed probe re-opens; otherwise only count the transition from closed
            if half_open or self.opened_at is None:
                self.times_opened += 1
            self.opened_at = time.monotonic()


def backoff_seconds(attempt: int, base: float = 0.05, cap: float = 2.0) -> float:
    # "full jitter" from the AWS backoff post
    return random.uniform(0, min(cap, base * 2**attempt))
