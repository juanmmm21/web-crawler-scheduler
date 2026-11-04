from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import aiohttp
import pytest

from tests.conftest import FakeClientSession, FakeResponse
from web_crawler_scheduler.fetcher import AiohttpFetcher
from web_crawler_scheduler.models import CacheValidators, PageRecord
from web_crawler_scheduler.rate_limiter import DomainRateLimiter
from web_crawler_scheduler.recrawl import (
    ChangeRatePolicy,
    RecrawlPipeline,
    RecrawlState,
    RecrawlStore,
    content_sha256,
)

T0 = datetime(2026, 11, 1, tzinfo=UTC)
URL = "http://example.com/page"


class _Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class _AllowAll:
    async def is_allowed(self, url: str, user_agent: str) -> bool:
        return True

    async def crawl_delay(self, url: str, user_agent: str) -> float | None:
        return None


class _DenyAll(_AllowAll):
    async def is_allowed(self, url: str, user_agent: str) -> bool:
        return False


def _state(**overrides: object) -> RecrawlState:
    values: dict[str, object] = {
        "url": URL,
        "depth": 1,
        "etag": '"v1"',
        "last_modified": None,
        "content_sha256": content_sha256("<p>v1</p>"),
        "last_fetched_at": T0,
        "next_due_at": T0 + timedelta(days=1),
    }
    values.update(overrides)
    return RecrawlState(**values)  # type: ignore[arg-type]


def _pipeline(
    store: RecrawlStore, responses: dict[str, object], clock: _Clock, robots: object = None
) -> tuple[RecrawlPipeline, FakeClientSession]:
    session = FakeClientSession(responses)  # type: ignore[arg-type]
    fetcher = AiohttpFetcher(cast(aiohttp.ClientSession, session), max_retries=0)
    pipeline = RecrawlPipeline(
        store,
        fetcher,
        DomainRateLimiter(max_concurrent_per_domain=1, default_min_delay_seconds=0.0),
        robots if robots is not None else _AllowAll(),  # type: ignore[arg-type]
        policy=ChangeRatePolicy(),
        user_agent="test",
        clock=clock,
    )
    return pipeline, session


class TestChangeRatePolicy:
    def test_first_interval_is_initial(self) -> None:
        assert ChangeRatePolicy().next_interval(0, 0, 0.0) == timedelta(days=1)

    def test_estimator_matches_hand_computation(self) -> None:
        # 4 visitas, 2 cambios, intervalo medio 1 día: -ln(2.5/4.5) / 86400
        rate = ChangeRatePolicy().estimated_change_rate(4, 2, 86_400.0)
        assert math.isclose(rate, -math.log(2.5 / 4.5) / 86_400.0)

    def test_pages_that_never_change_drift_to_max_interval(self) -> None:
        policy = ChangeRatePolicy()
        assert policy.next_interval(30, 0, 30 * 86_400.0) == policy.max_interval

    def test_growth_is_gradual_for_stable_pages(self) -> None:
        policy = ChangeRatePolicy()
        assert policy.next_interval(1, 0, 86_400.0, 86_400.0) == timedelta(days=2)
        assert policy.next_interval(2, 0, 3 * 86_400.0, 2 * 86_400.0) == timedelta(days=4)

    def test_pages_that_always_change_are_revisited_more_often(self) -> None:
        policy = ChangeRatePolicy()
        always = policy.next_interval(10, 10, 10 * 86_400.0)
        sometimes = policy.next_interval(10, 3, 10 * 86_400.0)
        assert policy.min_interval <= always < sometimes < policy.max_interval

    def test_invalid_bounds(self) -> None:
        with pytest.raises(ValueError):
            ChangeRatePolicy(min_interval=timedelta(days=2), initial_interval=timedelta(days=1))
        with pytest.raises(ValueError):
            ChangeRatePolicy(target_change_probability=1.0)


class TestCacheValidators:
    def test_headers_are_case_insensitive(self) -> None:
        validators = CacheValidators.from_headers({"etag": '"x"', "LAST-MODIFIED": "Mon"})
        assert validators.request_headers() == {"If-None-Match": '"x"', "If-Modified-Since": "Mon"}
        assert CacheValidators.from_headers({}).is_empty


class TestRecrawlPipeline:
    async def test_not_modified_sends_validators_and_keeps_content(self, tmp_path: Path) -> None:
        store = RecrawlStore([_state()])
        clock = _Clock(T0 + timedelta(days=2))
        pipeline, session = _pipeline(store, {URL: FakeResponse(status=304, final_url=URL)}, clock)

        report = await pipeline.run(tmp_path / "changed.jsonl", tmp_path / "removed.jsonl")

        assert report.checked == 1 and report.not_modified == 1 and report.changed == 0
        assert session.requested_headers[0] is not None
        assert session.requested_headers[0]["If-None-Match"] == '"v1"'
        state = store.get(URL)
        assert state is not None and state.checks == 1 and state.changes == 0
        assert state.next_due_at > clock.now + timedelta(days=1)
        assert (tmp_path / "changed.jsonl").read_text(encoding="utf-8") == ""

    async def test_changed_page_is_emitted_as_page_record(self, tmp_path: Path) -> None:
        store = RecrawlStore([_state()])
        clock = _Clock(T0 + timedelta(days=2))
        response = FakeResponse(body="<p>v2</p>", headers={"ETag": '"v2"'}, final_url=URL)
        pipeline, _ = _pipeline(store, {URL: response}, clock)

        report = await pipeline.run(tmp_path / "changed.jsonl", tmp_path / "removed.jsonl")

        assert report.changed == 1
        (line,) = (tmp_path / "changed.jsonl").read_text(encoding="utf-8").splitlines()
        page = PageRecord.from_json_dict(json.loads(line))
        assert page.html == "<p>v2</p>" and page.depth == 1 and page.fetched_at == clock.now
        state = store.get(URL)
        assert state is not None and state.etag == '"v2"' and state.changes == 1

    async def test_same_body_without_validators_counts_as_unchanged(self, tmp_path: Path) -> None:
        store = RecrawlStore([_state(etag=None)])
        pipeline, session = _pipeline(
            store,
            {URL: FakeResponse(body="<p>v1</p>", final_url=URL)},
            _Clock(T0 + timedelta(days=2)),
        )
        report = await pipeline.run(tmp_path / "c.jsonl", tmp_path / "r.jsonl")
        assert report.unchanged_body == 1
        assert "If-None-Match" not in (session.requested_headers[0] or {})

    async def test_gone_page_is_removed_and_reported(self, tmp_path: Path) -> None:
        store = RecrawlStore([_state()])
        pipeline, _ = _pipeline(
            store, {URL: FakeResponse(status=410)}, _Clock(T0 + timedelta(days=2))
        )
        report = await pipeline.run(tmp_path / "c.jsonl", tmp_path / "r.jsonl")
        assert report.removed == 1 and store.get(URL) is None
        assert json.loads((tmp_path / "r.jsonl").read_text("utf-8")) == {
            "url": URL,
            "status_code": 410,
        }

    async def test_transient_failure_is_retried_later_without_counting(
        self, tmp_path: Path
    ) -> None:
        store = RecrawlStore([_state()])
        clock = _Clock(T0 + timedelta(days=2))
        pipeline, _ = _pipeline(store, {URL: FakeResponse(status=503)}, clock)
        report = await pipeline.run(tmp_path / "c.jsonl", tmp_path / "r.jsonl")
        state = store.get(URL)
        assert report.failed == 1 and state is not None and state.checks == 0
        assert state.next_due_at == clock.now + ChangeRatePolicy().min_interval

    async def test_robots_disallow_stops_recrawling(self, tmp_path: Path) -> None:
        store = RecrawlStore([_state()])
        pipeline, session = _pipeline(store, {}, _Clock(T0 + timedelta(days=2)), robots=_DenyAll())
        report = await pipeline.run(tmp_path / "c.jsonl", tmp_path / "r.jsonl")
        assert report.robots_disallowed == 1 and session.requested_urls == []
        assert store.get(URL) is None

    async def test_only_due_pages_are_visited_oldest_first(self, tmp_path: Path) -> None:
        early = _state(url="http://example.com/a", next_due_at=T0)
        late = _state(url="http://example.com/b", next_due_at=T0 + timedelta(hours=1))
        future = _state(url="http://example.com/c", next_due_at=T0 + timedelta(days=9))
        store = RecrawlStore([future, late, early])
        responses = {s.url: FakeResponse(status=304, final_url=s.url) for s in (early, late)}
        pipeline, session = _pipeline(store, responses, _Clock(T0 + timedelta(hours=2)))  # type: ignore[arg-type]
        report = await pipeline.run(tmp_path / "c.jsonl", tmp_path / "r.jsonl", max_pages=1)
        assert report.checked == 1 and session.requested_urls == ["http://example.com/a"]


class TestRecrawlStore:
    def test_seed_from_pages_and_round_trip(self, tmp_path: Path) -> None:
        page = PageRecord(
            url=URL,
            final_url=URL,
            status_code=200,
            headers={"Last-Modified": "Sun, 01 Nov 2026"},
            html="<p>v1</p>",
            fetched_at=T0,
            depth=2,
            content_type="text/html",
        )
        pages = tmp_path / "pages.jsonl"
        pages.write_text(json.dumps(page.to_json_dict()) + "\nnot json\n", encoding="utf-8")
        store = RecrawlStore()
        assert store.seed_from_pages(pages, ChangeRatePolicy()) == 1
        assert store.seed_from_pages(pages, ChangeRatePolicy()) == 0
        store.save(tmp_path / "state.json")
        loaded = RecrawlStore.load(tmp_path / "state.json")
        state = loaded.get(URL)
        assert state is not None and state.last_modified == "Sun, 01 Nov 2026"
        assert state.next_due_at == T0 + timedelta(days=1)

    def test_unknown_state_version_fails(self, tmp_path: Path) -> None:
        (tmp_path / "s.json").write_text('{"format_version": 9, "pages": []}', encoding="utf-8")
        with pytest.raises(ValueError):
            RecrawlStore.load(tmp_path / "s.json")
