# AGENTS.md — web-crawler-scheduler

Directrices específicas para este repositorio. Hereda reglas globales de `../AGENTS.md` y `../CLAUDE.md`.

---

## Posición en el ecosistema

**Proyecto 1/10 — Categoría: Ingesta.** Es el punto de entrada de todo `beacon-search-engine`: sin páginas crawleadas no hay corpus, y sin corpus no hay nada que indexar ni rankear. Recorre la web de forma asíncrona y educada a partir de URLs semilla, respetando `robots.txt` y límites de *rate* por dominio, manteniendo una frontera de URLs priorizada y deduplicada.

## Integración con el resto del ecosistema

*   **Consume:** una lista de URLs semilla (config o CLI) y, opcionalmente, un checkpoint previo para reanudar un crawl interrumpido.
*   **Produce:** un conjunto de páginas crawleadas serializadas en **JSONL** (una página por línea: URL, HTML crudo, headers HTTP, código de estado, timestamp) más un **grafo de enlaces salientes** por página (lista de URLs destino), consumido después por `html-content-extractor` (contenido) y `pagerank-link-analysis` (grafo).
*   La integración real con los demás subproyectos ocurre dentro de `beacon-search-console`, no vía imports directos entre estos repos — cada uno consume/produce archivos, nunca código de otro repo.

## Stack sugerido

*   Python `>=3.11`, `pyproject.toml` + `hatchling`
*   `asyncio` + `aiohttp` (o `httpx` async) para las peticiones concurrentes
*   `mypy --strict`, `ruff`, `pytest`
*   Estructura estándar: `src/web_crawler_scheduler/{models,protocols,pipeline,__main__}.py`, `tests/`

## Definition of Done (alto nivel)

*   [ ] Parser de `robots.txt` con respeto de `Disallow` y `Crawl-delay`
*   [ ] Frontera de URLs con prioridad configurable (BFS por defecto) y límite de profundidad
*   [ ] Deduplicación de URLs por hash normalizado (esquema, host, path, query ordenada)
*   [ ] *Rate limiting* por dominio (peticiones concurrentes máximas + delay mínimo entre peticiones al mismo host)
*   [ ] *Backoff* exponencial ante 429/5xx y timeouts; descarte explícito y registrado tras agotar reintentos
*   [ ] Extracción de grafo de enlaces salientes por página (para `pagerank-link-analysis`)
*   [ ] Checkpoint/reanudación de crawls largos sin perder el estado de la frontera
*   [ ] Salida en JSONL documentada con schema explícito (Pydantic/dataclass serializable)

## Nota sobre git

El nombre de esta carpeta debe coincidir exactamente con el repo `github.com/juanmmm21/web-crawler-scheduler` cuando Juan lo cree. Hasta entonces, no ejecutar ningún comando de git aquí.
