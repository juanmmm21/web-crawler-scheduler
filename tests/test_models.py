from datetime import UTC, datetime

import pytest

from web_crawler_scheduler.models import (
    CheckpointState,
    CrawlConfig,
    DiscardedUrl,
    FetchOutcome,
    FrontierEntry,
    LinkGraphEntry,
    PageRecord,
)


class TestCrawlConfigValidation:
    def test_valid_config_constructs(self) -> None:
        config = CrawlConfig(seed_urls=("http://example.com/",))
        assert config.max_pages == 1000

    def test_rejects_empty_seed_urls(self) -> None:
        with pytest.raises(ValueError):
            CrawlConfig(seed_urls=())

    def test_rejects_non_positive_max_pages(self) -> None:
        with pytest.raises(ValueError):
            CrawlConfig(seed_urls=("http://example.com/",), max_pages=0)

    def test_rejects_negative_max_depth(self) -> None:
        with pytest.raises(ValueError):
            CrawlConfig(seed_urls=("http://example.com/",), max_depth=-1)

    def test_rejects_non_positive_concurrency(self) -> None:
        with pytest.raises(ValueError):
            CrawlConfig(seed_urls=("http://example.com/",), max_concurrent_requests=0)

    def test_rejects_non_positive_per_domain_concurrency(self) -> None:
        with pytest.raises(ValueError):
            CrawlConfig(seed_urls=("http://example.com/",), max_concurrent_per_domain=0)

    def test_rejects_negative_default_delay(self) -> None:
        with pytest.raises(ValueError):
            CrawlConfig(seed_urls=("http://example.com/",), default_min_delay_seconds=-1.0)

    def test_rejects_non_positive_timeout(self) -> None:
        with pytest.raises(ValueError):
            CrawlConfig(seed_urls=("http://example.com/",), request_timeout_seconds=0.0)

    def test_rejects_negative_max_retries(self) -> None:
        with pytest.raises(ValueError):
            CrawlConfig(seed_urls=("http://example.com/",), max_retries=-1)

    def test_rejects_non_positive_backoff_base(self) -> None:
        with pytest.raises(ValueError):
            CrawlConfig(seed_urls=("http://example.com/",), backoff_base_seconds=0.0)

    def test_rejects_backoff_max_below_base(self) -> None:
        with pytest.raises(ValueError):
            CrawlConfig(
                seed_urls=("http://example.com/",),
                backoff_base_seconds=5.0,
                backoff_max_seconds=1.0,
            )


class TestPageRecordJsonRoundTrip:
    def test_round_trip_preserves_fields(self) -> None:
        page = PageRecord(
            url="http://example.com/",
            final_url="http://example.com/",
            status_code=200,
            headers={"Content-Type": "text/html"},
            html="<html></html>",
            fetched_at=datetime(2026, 1, 1, tzinfo=UTC),
            depth=0,
            content_type="text/html",
        )
        restored = PageRecord.from_json_dict(page.to_json_dict())
        assert restored == page

    def test_round_trip_preserves_none_content_type(self) -> None:
        page = PageRecord(
            url="http://example.com/",
            final_url="http://example.com/",
            status_code=200,
            headers={},
            html="",
            fetched_at=datetime(2026, 1, 1, tzinfo=UTC),
            depth=0,
            content_type=None,
        )
        restored = PageRecord.from_json_dict(page.to_json_dict())
        assert restored.content_type is None


class TestLinkGraphEntryJsonRoundTrip:
    def test_round_trip_preserves_outlinks(self) -> None:
        entry = LinkGraphEntry(url="http://example.com/", outlinks=("http://example.com/a",))
        restored = LinkGraphEntry.from_json_dict(entry.to_json_dict())
        assert restored == entry


class TestDiscardedUrlJson:
    def test_to_json_dict_serializes_outcome_value(self) -> None:
        discarded = DiscardedUrl(
            url="http://example.com/",
            reason="robots",
            outcome=FetchOutcome.ROBOTS_DISALLOWED,
            attempts=0,
            discarded_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        data = discarded.to_json_dict()
        assert data["outcome"] == "robots_disallowed"


class TestCheckpointStateJsonRoundTrip:
    def test_round_trip_preserves_state(self) -> None:
        state = CheckpointState(
            visited_hashes={"abc", "def"},
            frontier_entries=[
                FrontierEntry(url="http://example.com/a", depth=1, priority=1, discovered_from="http://example.com/")
            ],
            pages_crawled=3,
        )
        restored = CheckpointState.from_json_dict(state.to_json_dict())
        assert restored.visited_hashes == state.visited_hashes
        assert restored.frontier_entries == state.frontier_entries
        assert restored.pages_crawled == state.pages_crawled

    def test_defaults_are_empty(self) -> None:
        state = CheckpointState()
        assert state.visited_hashes == set()
        assert state.frontier_entries == []
        assert state.pages_crawled == 0
