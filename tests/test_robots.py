import aiohttp

from web_crawler_scheduler.robots import RobotsCache, _parse_crawl_delay

from .conftest import FakeClientSession, FakeResponse

USER_AGENT = "BeaconCrawler/0.1"


class TestRobotsCache:
    async def test_allows_when_no_disallow_rule(self) -> None:
        session = FakeClientSession(
            {
                "http://example.com/robots.txt": FakeResponse(
                    status=200, body="User-agent: *\nAllow: /\n"
                )
            }
        )
        cache = RobotsCache(session)  # type: ignore[arg-type]
        assert await cache.is_allowed("http://example.com/page", USER_AGENT) is True

    async def test_disallows_blocked_path(self) -> None:
        session = FakeClientSession(
            {
                "http://example.com/robots.txt": FakeResponse(
                    status=200, body="User-agent: *\nDisallow: /private/\n"
                )
            }
        )
        cache = RobotsCache(session)  # type: ignore[arg-type]
        assert await cache.is_allowed("http://example.com/private/secret", USER_AGENT) is False
        assert await cache.is_allowed("http://example.com/public", USER_AGENT) is True

    async def test_missing_robots_txt_allows_everything(self) -> None:
        session = FakeClientSession(
            {"http://example.com/robots.txt": FakeResponse(status=404)}
        )
        cache = RobotsCache(session)  # type: ignore[arg-type]
        assert await cache.is_allowed("http://example.com/anything", USER_AGENT) is True

    async def test_client_error_status_allows_everything(self) -> None:
        session = FakeClientSession(
            {"http://example.com/robots.txt": FakeResponse(status=403)}
        )
        cache = RobotsCache(session)  # type: ignore[arg-type]
        assert await cache.is_allowed("http://example.com/anything", USER_AGENT) is True

    async def test_server_error_disallows_everything(self) -> None:
        session = FakeClientSession(
            {"http://example.com/robots.txt": FakeResponse(status=503)}
        )
        cache = RobotsCache(session)  # type: ignore[arg-type]
        assert await cache.is_allowed("http://example.com/anything", USER_AGENT) is False

    async def test_connection_error_disallows_everything(self) -> None:
        session = FakeClientSession(
            {
                "http://example.com/robots.txt": FakeResponse(
                    raise_on_enter=aiohttp.ClientConnectionError("boom")
                )
            }
        )
        cache = RobotsCache(session)  # type: ignore[arg-type]
        assert await cache.is_allowed("http://example.com/anything", USER_AGENT) is False

    async def test_timeout_disallows_everything(self) -> None:
        session = FakeClientSession(
            {"http://example.com/robots.txt": FakeResponse(raise_on_enter=TimeoutError())}
        )
        cache = RobotsCache(session)  # type: ignore[arg-type]
        assert await cache.is_allowed("http://example.com/anything", USER_AGENT) is False

    async def test_extracts_crawl_delay(self) -> None:
        session = FakeClientSession(
            {
                "http://example.com/robots.txt": FakeResponse(
                    status=200, body="User-agent: *\nCrawl-delay: 2.5\n"
                )
            }
        )
        cache = RobotsCache(session)  # type: ignore[arg-type]
        delay = await cache.crawl_delay("http://example.com/page", USER_AGENT)
        assert delay == 2.5

    async def test_no_crawl_delay_directive_returns_none(self) -> None:
        session = FakeClientSession(
            {
                "http://example.com/robots.txt": FakeResponse(
                    status=200, body="User-agent: *\nAllow: /\n"
                )
            }
        )
        cache = RobotsCache(session)  # type: ignore[arg-type]
        assert await cache.crawl_delay("http://example.com/page", USER_AGENT) is None

    async def test_robots_txt_fetched_once_per_origin(self) -> None:
        session = FakeClientSession(
            {
                "http://example.com/robots.txt": FakeResponse(
                    status=200, body="User-agent: *\nDisallow: /private/\n"
                )
            }
        )
        cache = RobotsCache(session)  # type: ignore[arg-type]
        await cache.is_allowed("http://example.com/a", USER_AGENT)
        await cache.is_allowed("http://example.com/b", USER_AGENT)
        await cache.crawl_delay("http://example.com/c", USER_AGENT)
        assert session.requested_urls == ["http://example.com/robots.txt"]

    async def test_different_origins_are_cached_independently(self) -> None:
        session = FakeClientSession(
            {
                "http://a.example.com/robots.txt": FakeResponse(
                    status=200, body="User-agent: *\nDisallow: /\n"
                ),
                "http://b.example.com/robots.txt": FakeResponse(
                    status=200, body="User-agent: *\nAllow: /\n"
                ),
            }
        )
        cache = RobotsCache(session)  # type: ignore[arg-type]
        assert await cache.is_allowed("http://a.example.com/page", USER_AGENT) is False
        assert await cache.is_allowed("http://b.example.com/page", USER_AGENT) is True


class TestParseCrawlDelay:
    def test_fractional_delay_is_parsed(self) -> None:
        body = "User-agent: *\nCrawl-delay: 0.5\n"
        assert _parse_crawl_delay(body, USER_AGENT) == 0.5

    def test_exact_user_agent_takes_priority_over_wildcard(self) -> None:
        body = (
            "User-agent: BeaconCrawler\nCrawl-delay: 5\n\n"
            "User-agent: *\nCrawl-delay: 1\n"
        )
        assert _parse_crawl_delay(body, "BeaconCrawler/0.1") == 5.0

    def test_falls_back_to_wildcard_when_no_exact_match(self) -> None:
        body = "User-agent: OtherBot\nCrawl-delay: 5\n\nUser-agent: *\nCrawl-delay: 1\n"
        assert _parse_crawl_delay(body, "BeaconCrawler/0.1") == 1.0

    def test_grouped_user_agents_share_directives(self) -> None:
        body = "User-agent: BeaconCrawler\nUser-agent: OtherBot\nCrawl-delay: 3\n"
        assert _parse_crawl_delay(body, "beaconcrawler") == 3.0
        assert _parse_crawl_delay(body, "otherbot") == 3.0

    def test_no_directive_returns_none(self) -> None:
        assert _parse_crawl_delay("User-agent: *\nAllow: /\n", USER_AGENT) is None
