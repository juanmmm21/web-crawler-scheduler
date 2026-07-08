from web_crawler_scheduler.link_extractor import extract_outlinks


class TestExtractOutlinks:
    def test_extracts_absolute_links(self) -> None:
        html = '<a href="http://other.com/a">A</a><a href="https://other.com/b">B</a>'
        links = extract_outlinks(html, base_url="http://example.com/")
        assert links == ["http://other.com/a", "https://other.com/b"]

    def test_resolves_relative_links_against_base(self) -> None:
        html = '<a href="/relative">rel</a><a href="child">child</a>'
        links = extract_outlinks(html, base_url="http://example.com/dir/page.html")
        assert links == ["http://example.com/relative", "http://example.com/dir/child"]

    def test_filters_non_http_schemes(self) -> None:
        html = (
            '<a href="mailto:test@example.com">mail</a>'
            '<a href="javascript:void(0)">js</a>'
            '<a href="tel:+123456">tel</a>'
            '<a href="#section">anchor</a>'
            '<a href="http://example.com/ok">ok</a>'
        )
        links = extract_outlinks(html, base_url="http://example.com/")
        assert links == ["http://example.com/ok"]

    def test_deduplicates_preserving_first_occurrence_order(self) -> None:
        html = (
            '<a href="http://example.com/a">first</a>'
            '<a href="http://example.com/b">second</a>'
            '<a href="http://example.com/a">dup</a>'
        )
        links = extract_outlinks(html, base_url="http://example.com/")
        assert links == ["http://example.com/a", "http://example.com/b"]

    def test_ignores_anchors_without_href(self) -> None:
        html = '<a name="no-href">no link</a><a href="http://example.com/ok">ok</a>'
        links = extract_outlinks(html, base_url="http://example.com/")
        assert links == ["http://example.com/ok"]

    def test_tolerates_malformed_html(self) -> None:
        html = '<div><a href="http://example.com/ok">unclosed<p>broken'
        links = extract_outlinks(html, base_url="http://example.com/")
        assert links == ["http://example.com/ok"]

    def test_empty_html_returns_no_links(self) -> None:
        assert extract_outlinks("", base_url="http://example.com/") == []

    def test_no_anchors_returns_no_links(self) -> None:
        html = "<html><body><p>no links here</p></body></html>"
        assert extract_outlinks(html, base_url="http://example.com/") == []
