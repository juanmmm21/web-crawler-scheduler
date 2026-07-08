"""Extracción de enlaces salientes (`<a href>`) de HTML crudo, para el grafo de enlaces.

Se usa `html.parser.HTMLParser` de la librería estándar en vez de una
librería de parsing de terceros (BeautifulSoup, lxml): el crawler solo
necesita recolectar atributos `href` de etiquetas `<a>`, no un DOM completo, y
`HTMLParser` ya tolera de forma nativa HTML mal formado (tags sin cerrar,
anidamiento inválido) sin necesitar una dependencia adicional.
"""

from __future__ import annotations

from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

_LINKABLE_SCHEMES = frozenset({"http", "https"})


class _AnchorHrefParser(HTMLParser):
    """Recolecta los valores `href` de todas las etiquetas `<a>` del HTML, en orden."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value:
                self.hrefs.append(value)


def extract_outlinks(html: str, base_url: str) -> list[str]:
    """Extrae URLs absolutas http(s) únicas enlazadas desde `html`, en orden de aparición.

    Los enlaces relativos se resuelven contra `base_url` (la URL final tras
    redirecciones); los esquemas no navegables (`mailto:`, `javascript:`,
    `tel:`, anclas puras...) se descartan porque no aportan nada al grafo de
    enlaces ni son crawleables.
    """
    parser = _AnchorHrefParser()
    parser.feed(html)
    parser.close()

    seen: set[str] = set()
    outlinks: list[str] = []
    for raw_href in parser.hrefs:
        href = raw_href.strip()
        if not href or href.startswith("#"):
            continue  # ancla a la misma página: no es un recurso nuevo que crawlear
        absolute = urljoin(base_url, href)
        if urlsplit(absolute).scheme not in _LINKABLE_SCHEMES:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        outlinks.append(absolute)
    return outlinks
