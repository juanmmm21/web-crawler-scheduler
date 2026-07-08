import aiohttp
import pytest

from web_crawler_scheduler.fetcher import AiohttpFetcher, FetchError

from .conftest import FakeClientSession, FakeResponse

FAST_KWARGS = {"backoff_base_seconds": 0.001, "backoff_max_seconds": 0.01}


class TestAiohttpFetcherValidation:
    def test_rejects_negative_max_retries(self) -> None:
        with pytest.raises(ValueError):
            AiohttpFetcher(FakeClientSession({}), max_retries=-1)  # type: ignore[arg-type]

    def test_rejects_non_positive_backoff_base(self) -> None:
        with pytest.raises(ValueError):
            AiohttpFetcher(
                FakeClientSession({}), backoff_base_seconds=0.0  # type: ignore[arg-type]
            )

    def test_rejects_backoff_max_below_base(self) -> None:
        with pytest.raises(ValueError):
            AiohttpFetcher(
                FakeClientSession({}),  # type: ignore[arg-type]
                backoff_base_seconds=5.0,
                backoff_max_seconds=1.0,
            )


class TestAiohttpFetcherSuccess:
    async def test_returns_fetch_result_on_success(self) -> None:
        session = FakeClientSession(
            {
                "http://example.com/page": FakeResponse(
                    status=200,
                    body="<html>hi</html>",
                    headers={"Content-Type": "text/html"},
                    content_type="text/html",
                    final_url="http://example.com/page",
                )
            }
        )
        fetcher = AiohttpFetcher(session, **FAST_KWARGS)  # type: ignore[arg-type]
        result = await fetcher.fetch("http://example.com/page", timeout_seconds=5.0)

        assert result.status_code == 200
        assert result.body == "<html>hi</html>"
        assert result.final_url == "http://example.com/page"
        assert result.content_type == "text/html"

    async def test_sends_configured_user_agent(self) -> None:
        session = FakeClientSession(
            {"http://example.com/page": FakeResponse(status=200, body="ok")}
        )
        fetcher = AiohttpFetcher(session, user_agent="BeaconCrawler/0.1", **FAST_KWARGS)  # type: ignore[arg-type]
        await fetcher.fetch("http://example.com/page", timeout_seconds=5.0)

        assert session.requested_headers[0] == {"User-Agent": "BeaconCrawler/0.1"}


class TestAiohttpFetcherRetries:
    async def test_retries_retryable_status_then_succeeds(self) -> None:
        session = FakeClientSession(
            {
                "http://example.com/page": [
                    FakeResponse(status=503),
                    FakeResponse(status=200, body="recovered"),
                ]
            }
        )
        fetcher = AiohttpFetcher(session, max_retries=3, **FAST_KWARGS)  # type: ignore[arg-type]
        result = await fetcher.fetch("http://example.com/page", timeout_seconds=5.0)

        assert result.body == "recovered"
        assert len(session.requested_urls) == 2

    async def test_retries_connection_error_then_succeeds(self) -> None:
        session = FakeClientSession(
            {
                "http://example.com/page": [
                    FakeResponse(raise_on_enter=aiohttp.ClientConnectionError("boom")),
                    FakeResponse(status=200, body="recovered"),
                ]
            }
        )
        fetcher = AiohttpFetcher(session, max_retries=3, **FAST_KWARGS)  # type: ignore[arg-type]
        result = await fetcher.fetch("http://example.com/page", timeout_seconds=5.0)

        assert result.body == "recovered"

    async def test_exhausts_retries_on_persistent_server_error(self) -> None:
        session = FakeClientSession({"http://example.com/page": FakeResponse(status=503)})
        fetcher = AiohttpFetcher(session, max_retries=2, **FAST_KWARGS)  # type: ignore[arg-type]

        with pytest.raises(FetchError) as exc_info:
            await fetcher.fetch("http://example.com/page", timeout_seconds=5.0)

        assert exc_info.value.attempts == 3
        assert len(session.requested_urls) == 3

    async def test_exhausts_retries_on_persistent_timeout(self) -> None:
        session = FakeClientSession(
            {"http://example.com/page": FakeResponse(raise_on_enter=TimeoutError())}
        )
        fetcher = AiohttpFetcher(session, max_retries=1, **FAST_KWARGS)  # type: ignore[arg-type]

        with pytest.raises(FetchError) as exc_info:
            await fetcher.fetch("http://example.com/page", timeout_seconds=5.0)

        assert exc_info.value.attempts == 2

    async def test_permanent_client_error_is_not_retried(self) -> None:
        session = FakeClientSession({"http://example.com/missing": FakeResponse(status=404)})
        fetcher = AiohttpFetcher(session, max_retries=3, **FAST_KWARGS)  # type: ignore[arg-type]

        with pytest.raises(FetchError) as exc_info:
            await fetcher.fetch("http://example.com/missing", timeout_seconds=5.0)

        assert exc_info.value.attempts == 1
        assert len(session.requested_urls) == 1

    async def test_zero_retries_fails_after_single_attempt(self) -> None:
        session = FakeClientSession({"http://example.com/page": FakeResponse(status=503)})
        fetcher = AiohttpFetcher(session, max_retries=0, **FAST_KWARGS)  # type: ignore[arg-type]

        with pytest.raises(FetchError) as exc_info:
            await fetcher.fetch("http://example.com/page", timeout_seconds=5.0)

        assert exc_info.value.attempts == 1
        assert len(session.requested_urls) == 1
