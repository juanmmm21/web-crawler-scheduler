import asyncio
import time

import pytest

from web_crawler_scheduler.rate_limiter import DomainRateLimiter


class TestDomainRateLimiterValidation:
    def test_rejects_non_positive_concurrency(self) -> None:
        with pytest.raises(ValueError):
            DomainRateLimiter(max_concurrent_per_domain=0, default_min_delay_seconds=0.0)

    def test_rejects_negative_default_delay(self) -> None:
        with pytest.raises(ValueError):
            DomainRateLimiter(max_concurrent_per_domain=1, default_min_delay_seconds=-1.0)


class TestDomainRateLimiterDelay:
    async def test_enforces_minimum_delay_between_requests(self) -> None:
        limiter = DomainRateLimiter(max_concurrent_per_domain=5, default_min_delay_seconds=0.05)
        url = "http://example.com/page"

        await limiter.acquire(url)
        limiter.release(url)
        start = time.monotonic()
        await limiter.acquire(url)
        elapsed = time.monotonic() - start
        limiter.release(url)

        assert elapsed >= 0.045

    async def test_per_call_delay_overrides_default(self) -> None:
        limiter = DomainRateLimiter(max_concurrent_per_domain=5, default_min_delay_seconds=0.0)
        url = "http://example.com/page"

        await limiter.acquire(url, min_delay_seconds=0.05)
        limiter.release(url)
        start = time.monotonic()
        await limiter.acquire(url, min_delay_seconds=0.05)
        elapsed = time.monotonic() - start
        limiter.release(url)

        assert elapsed >= 0.045

    async def test_rejects_negative_per_call_delay(self) -> None:
        limiter = DomainRateLimiter(max_concurrent_per_domain=1, default_min_delay_seconds=0.0)
        with pytest.raises(ValueError):
            await limiter.acquire("http://example.com/", min_delay_seconds=-1.0)

    async def test_different_domains_do_not_share_delay(self) -> None:
        limiter = DomainRateLimiter(max_concurrent_per_domain=5, default_min_delay_seconds=1.0)
        await limiter.acquire("http://a.example.com/")
        limiter.release("http://a.example.com/")

        start = time.monotonic()
        await limiter.acquire("http://b.example.com/")
        elapsed = time.monotonic() - start
        limiter.release("http://b.example.com/")

        assert elapsed < 0.2


class TestDomainRateLimiterConcurrency:
    async def test_blocks_beyond_max_concurrency(self) -> None:
        limiter = DomainRateLimiter(max_concurrent_per_domain=1, default_min_delay_seconds=0.0)
        url = "http://example.com/page"
        events: list[str] = []

        async def holder() -> None:
            await limiter.acquire(url)
            events.append("A-acquired")
            await asyncio.sleep(0.05)
            events.append("A-released")
            limiter.release(url)

        async def waiter() -> None:
            await asyncio.sleep(0.01)
            await limiter.acquire(url)
            events.append("B-acquired")
            limiter.release(url)

        await asyncio.gather(holder(), waiter())

        assert events.index("A-released") < events.index("B-acquired")

    async def test_allows_concurrency_up_to_limit(self) -> None:
        limiter = DomainRateLimiter(max_concurrent_per_domain=2, default_min_delay_seconds=0.0)
        url = "http://example.com/page"
        concurrent_count = 0
        max_observed = 0

        async def worker() -> None:
            nonlocal concurrent_count, max_observed
            await limiter.acquire(url)
            concurrent_count += 1
            max_observed = max(max_observed, concurrent_count)
            await asyncio.sleep(0.02)
            concurrent_count -= 1
            limiter.release(url)

        await asyncio.gather(worker(), worker(), worker())

        assert max_observed == 2


class TestDomainRateLimiterRelease:
    def test_release_without_acquire_raises(self) -> None:
        limiter = DomainRateLimiter(max_concurrent_per_domain=1, default_min_delay_seconds=0.0)
        with pytest.raises(RuntimeError):
            limiter.release("http://example.com/")
