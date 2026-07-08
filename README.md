# web-crawler-scheduler

**Proyecto 1/10 del ecosistema [`beacon-search-engine`](https://github.com/juanmmm21/beacon-search-engine)** — categoría *Ingesta*.
Repositorio: [`github.com/juanmmm21/web-crawler-scheduler`](https://github.com/juanmmm21/web-crawler-scheduler)

Un crawler web asíncrono y educado: recorre la web a partir de un conjunto de
URLs semilla, respeta `robots.txt` y aplica límites de *rate* por dominio,
mantiene una frontera de URLs priorizada y deduplicada, y produce como
resultado el HTML crudo de cada página junto con su grafo de enlaces
salientes — todo implementado desde cero, sin Scrapy ni ninguna librería de
crawling de terceros.

## Qué problema resuelve

Cualquier motor de búsqueda necesita primero un corpus. Obtenerlo de forma
correcta no es trivial: hay que evitar crawlear la misma URL dos veces por
variaciones triviales (mayúsculas, puerto por defecto, orden de query
params...), no saturar ni tumbar los servidores ajenos, recuperarse de fallos
de red transitorios sin perder progreso, y sobrevivir a HTML roto o
inalcanzable sin que se caiga todo el proceso. Este proyecto resuelve
exactamente esa capa: la ingesta educada y resiliente de páginas web.

## Rol en `beacon-search-engine`

```text
                        ┌──────────────────────────┐
                        │  web-crawler-scheduler    │   (este proyecto)
                        │  URLs semilla → páginas   │
                        └────────────┬─────────────┘
                                     │ pages.jsonl (HTML crudo + metadatos)
                                     │ link_graph.jsonl (grafo de enlaces)
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
      html-content-extractor              pagerank-link-analysis
      (limpia el HTML → texto)            (autoridad de página vía el grafo)
                    │
                    ▼
      inverted-index-builder → index-compression-codec → bm25-ranking-engine
                                                                   │
                                                                   ▼
                                                    (converge en beacon-search-console)
```

Es el punto de entrada de todo el ecosistema: sin páginas crawleadas no hay
corpus, y sin corpus no hay nada que indexar ni rankear.

## Objetivo y skills demostradas

- Programación asíncrona real con `asyncio`/`aiohttp`: miles de conexiones
  concurrentes controladas, no un bucle secuencial disfrazado de async.
- Gestión de una cola de prioridad (frontera BFS por defecto, priorizable).
- Cumplimiento ético de `robots.txt` (incluyendo `Crawl-delay` fraccionario,
  no soportado por `urllib.robotparser` de la librería estándar).
- Backoff exponencial con *jitter* ante errores transitorios de red.
- Deduplicación de URLs por hash normalizado a escala.
- Checkpointing y reanudación de trabajos de larga duración sin perder estado.

## Cómo funciona

1. Se cargan las URLs semilla en la **frontera** (`PriorityFrontier`), una
   cola de prioridad con desempate FIFO — por defecto ordena por profundidad
   de descubrimiento (BFS).
2. Un bucle principal lanza tareas concurrentes (hasta
   `max_concurrent_requests`) que van sacando entradas de la frontera.
3. Por cada URL: se comprueba deduplicación por hash normalizado
   (`urlnorm`), se consulta `robots.txt` (`RobotsCache`, cacheado por
   origen) y se adquiere hueco de *rate limiting* por dominio
   (`DomainRateLimiter`, que respeta tanto la concurrencia máxima como el
   `Crawl-delay` del sitio).
4. Se descarga la página (`AiohttpFetcher`), con reintentos y backoff
   exponencial ante 429/5xx/timeouts; un 4xx permanente o el agotamiento de
   reintentos descarta la URL de forma explícita y auditable.
5. Si la descarga tiene éxito, se extraen sus enlaces salientes
   (`link_extractor`, vía `html.parser` de la librería estándar) y se
   encolan los nuevos, respetando `max_depth`.
6. Todo se persiste en JSONL (`pages.jsonl`, `link_graph.jsonl`,
   `discarded.jsonl`) y, si se configura, se vuelca un checkpoint periódico
   para poder reanudar el crawl tras una interrupción.

## Arquitectura

```text
src/web_crawler_scheduler/
├── models.py          # dataclasses: CrawlConfig, PageRecord, LinkGraphEntry,
│                       # FrontierEntry, DiscardedUrl, CheckpointState, CrawlStats
├── protocols.py        # interfaces: RobotsPolicy, Frontier, Deduplicator,
│                       # RateLimiter, PageFetcher
├── urlnorm.py           # normalización de URLs + HashSetDeduplicator
├── robots.py             # RobotsCache: fetch, parseo y caché de robots.txt por origen
├── rate_limiter.py         # DomainRateLimiter: concurrencia y demora mínima por dominio
├── frontier.py              # PriorityFrontier: cola de prioridad BFS con desempate FIFO
├── fetcher.py                 # AiohttpFetcher: descarga con reintentos y backoff exponencial
├── link_extractor.py            # extracción de <a href> de HTML crudo
├── pipeline.py                    # CrawlPipeline: orquesta todo lo anterior
└── __main__.py                     # CLI (`crawl`, `stats`)
```

Cada módulo se testea de forma aislada mediante los protocolos definidos en
`protocols.py` y dobles de prueba en memoria (`tests/conftest.py`) — ningún
test golpea la red real.

## Requisitos e instalación

- Python `>=3.11`

```bash
git clone https://github.com/juanmmm21/web-crawler-scheduler.git
cd web-crawler-scheduler
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Uso (CLI)

```bash
# Crawl nuevo
web-crawler-scheduler crawl \
  --seed https://example.com/ \
  --output-dir ./output \
  --max-pages 500 \
  --max-depth 3 \
  --max-concurrent-requests 20 \
  --max-concurrent-per-domain 2 \
  --min-delay-seconds 1.0

# Reanudar un crawl interrumpido (usa el checkpoint.json de --output-dir)
web-crawler-scheduler crawl --seed https://example.com/ --output-dir ./output --resume

# Estadísticas rápidas de un pages.jsonl ya crawleado
web-crawler-scheduler stats ./output/pages.jsonl
```

También puede invocarse como módulo: `python -m web_crawler_scheduler crawl ...`.

Flags principales de `crawl`: `--seed` (repetible), `--seeds-file`,
`--output-dir`, `--max-pages`, `--max-depth`, `--max-concurrent-requests`,
`--max-concurrent-per-domain`, `--min-delay-seconds`, `--timeout-seconds`,
`--max-retries`, `--backoff-base-seconds`, `--backoff-max-seconds`,
`--user-agent`, `--ignore-robots`, `--checkpoint-every`, `--resume`.

## Formatos de datos

`--output-dir` produce tres ficheros JSONL (uno por línea = un registro) y un
checkpoint JSON:

**`pages.jsonl`** — una página crawleada con éxito:

```json
{
  "url": "http://example.com/",
  "final_url": "http://example.com/",
  "status_code": 200,
  "headers": {"Content-Type": "text/html; charset=utf-8"},
  "html": "<html>...</html>",
  "fetched_at": "2026-07-08T12:00:00+00:00",
  "depth": 0,
  "content_type": "text/html"
}
```

**`link_graph.jsonl`** — enlaces salientes de cada página (consumido por
[`pagerank-link-analysis`](https://github.com/juanmmm21/pagerank-link-analysis)):

```json
{"url": "http://example.com/", "outlinks": ["http://example.com/a", "http://example.com/b"]}
```

**`discarded.jsonl`** — URLs descartadas, para auditoría:

```json
{
  "url": "http://example.com/private",
  "reason": "bloqueada por robots.txt",
  "outcome": "robots_disallowed",
  "attempts": 0,
  "discarded_at": "2026-07-08T12:00:00+00:00"
}
```

**`checkpoint.json`** — estado interno para reanudar (hashes visitados,
frontera pendiente y páginas crawleadas); formato interno, no pensado para
consumo externo.

## Uso programático

```python
import asyncio
from pathlib import Path

import aiohttp

from web_crawler_scheduler.models import CrawlConfig
from web_crawler_scheduler.pipeline import CrawlPipeline


async def main() -> None:
    config = CrawlConfig(seed_urls=("https://example.com/",), max_pages=50, max_depth=2)
    output = Path("./output")
    async with aiohttp.ClientSession() as session:
        async with CrawlPipeline(
            config,
            session,
            pages_output_path=output / "pages.jsonl",
            link_graph_output_path=output / "link_graph.jsonl",
            discarded_output_path=output / "discarded.jsonl",
            checkpoint_path=output / "checkpoint.json",
        ) as pipeline:
            stats = await pipeline.run()
    print(stats.pages_crawled, stats.urls_discarded)


asyncio.run(main())
```

## Desarrollo

```bash
pytest
ruff check .
mypy --strict src/
```

Los tests no realizan peticiones de red reales: `tests/conftest.py` define
`FakeClientSession`/`FakeResponse`, dobles de prueba que implementan la misma
superficie que usa el código (`.get()` como *context manager* asíncrono) para
simular respuestas HTTP, errores de conexión y timeouts de forma determinista.

## Troubleshooting

- **El crawl no avanza / va muy lento:** revisa `--min-delay-seconds` y
  `--max-concurrent-per-domain` — si todas las semillas son del mismo
  dominio, el *rate limiting* por dominio es el cuello de botella esperado
  (es la política educada, no un bug).
- **`RuntimeError: release() llamado para un dominio sin acquire() previo`:**
  indica un uso incorrecto de `DomainRateLimiter` fuera de `CrawlPipeline`
  (llamar a `release()` sin un `acquire()` previo para ese dominio).
- **Un sitio nunca se crawlea:** comprueba `discarded.jsonl` — si el
  `outcome` es `robots_disallowed`, el propio `robots.txt` del sitio lo
  prohíbe; con `server_error`/`timeout` repetido, el origen puede estar
  caído (ver sección 5xx de `robots.py`, que asume "todo prohibido" ante un
  `robots.txt` inalcanzable, no solo ante las páginas).
- **`mypy` falla en `tests/`:** es esperado — solo `src/` se tipa en modo
  `--strict`; los dobles de prueba usan tipado más laxo deliberadamente.

## Roadmap

- [ ] Soporte de `sitemap.xml` como fuente adicional de URLs semilla.
- [ ] Content negotiation explícita (rechazar tipos MIME no HTML antes de
      descargar el cuerpo completo).
- [ ] Backend de checkpoint pluggable (hoy: fichero JSON local).

## Licencia

MIT — ver [`LICENSE`](./LICENSE).
