from web_crawler_scheduler.frontier import PriorityFrontier
from web_crawler_scheduler.models import FrontierEntry


def _entry(url: str, depth: int, priority: int | None = None) -> FrontierEntry:
    return FrontierEntry(url=url, depth=depth, priority=priority if priority is not None else depth)


class TestPriorityFrontier:
    def test_empty_frontier_pops_none(self) -> None:
        frontier = PriorityFrontier()
        assert frontier.pop() is None
        assert len(frontier) == 0

    def test_len_tracks_pending_entries(self) -> None:
        frontier = PriorityFrontier()
        frontier.push(_entry("http://example.com/a", depth=0))
        frontier.push(_entry("http://example.com/b", depth=0))
        assert len(frontier) == 2
        frontier.pop()
        assert len(frontier) == 1

    def test_bfs_default_pops_lower_depth_first(self) -> None:
        frontier = PriorityFrontier()
        frontier.push(_entry("http://example.com/deep", depth=2))
        frontier.push(_entry("http://example.com/shallow", depth=0))
        frontier.push(_entry("http://example.com/mid", depth=1))

        popped = [frontier.pop(), frontier.pop(), frontier.pop()]
        urls = [entry.url for entry in popped if entry is not None]
        assert urls == [
            "http://example.com/shallow",
            "http://example.com/mid",
            "http://example.com/deep",
        ]

    def test_same_priority_breaks_tie_by_insertion_order(self) -> None:
        frontier = PriorityFrontier()
        frontier.push(_entry("http://example.com/first", depth=0))
        frontier.push(_entry("http://example.com/second", depth=0))
        frontier.push(_entry("http://example.com/third", depth=0))

        urls = [frontier.pop().url for _ in range(3)]  # type: ignore[union-attr]
        assert urls == [
            "http://example.com/first",
            "http://example.com/second",
            "http://example.com/third",
        ]

    def test_custom_priority_overrides_depth(self) -> None:
        frontier = PriorityFrontier()
        frontier.push(_entry("http://example.com/low-priority", depth=0, priority=10))
        frontier.push(_entry("http://example.com/high-priority", depth=5, priority=1))

        first = frontier.pop()
        assert first is not None
        assert first.url == "http://example.com/high-priority"

    def test_to_entries_snapshot_matches_priority_order_without_mutating(self) -> None:
        frontier = PriorityFrontier()
        frontier.push(_entry("http://example.com/b", depth=1))
        frontier.push(_entry("http://example.com/a", depth=0))

        snapshot = frontier.to_entries()

        assert [entry.url for entry in snapshot] == [
            "http://example.com/a",
            "http://example.com/b",
        ]
        assert len(frontier) == 2  # el snapshot no consume la cola

    def test_bfs_priority_helper_matches_depth(self) -> None:
        assert PriorityFrontier.bfs_priority(0) == 0
        assert PriorityFrontier.bfs_priority(3) == 3
