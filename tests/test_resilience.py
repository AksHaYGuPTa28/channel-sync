import time

from app.resilience import CircuitBreaker, backoff_seconds


def test_breaker_opens_then_recovers():
    b = CircuitBreaker(failure_threshold=3, cooldown_sec=0.05)
    for _ in range(3):
        b.record_failure()
    assert b.state == "open"
    assert not b.allow()

    time.sleep(0.2)  # generous, Windows timer resolution is ~15ms
    assert b.state == "half_open"
    b.record_success()
    assert b.state == "closed"


def test_failed_probe_reopens():
    b = CircuitBreaker(failure_threshold=1, cooldown_sec=0.05)
    b.record_failure()
    time.sleep(0.2)
    b.record_failure()
    assert b.state == "open"
    assert b.times_opened == 2


def test_backoff_capped_with_jitter():
    values = {backoff_seconds(20, cap=1.0) for _ in range(50)}
    assert all(0 <= v <= 1.0 for v in values)
    assert len(values) > 1
