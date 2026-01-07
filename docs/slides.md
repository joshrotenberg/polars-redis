# Presentation

An interactive slide deck covering polars-redis features and usage.

<a href="slides/index.html" class="md-button md-button--primary" target="_blank">
    Open Presentation
</a>

!!! note "Direct Link"
    If the button above doesn't work, you can access the slides directly at:
    [https://joshrotenberg.github.io/polars-redis/slides/](https://joshrotenberg.github.io/polars-redis/slides/)

## Topics Covered

- The "Redis as your data layer" pitch
- Hero example: Redis as a first-class data source
- Scanning and writing DataFrames
- RediSearch query builder (Polars-like syntax)
- Server-side aggregation
- Smart scan (automatic index detection)
- DataFrame caching with `@cache` decorator
- Client operations (geo, keys, pipelines, transactions)
- Real-time data (Pub/Sub, Streams)
- Performance and architecture
- Use cases: data enrichment, ETL staging

## Keyboard Navigation

Once in the presentation:

| Key | Action |
|-----|--------|
| `Space` / `Arrow Right` | Next slide |
| `Arrow Left` | Previous slide |
| `Escape` | Overview mode |
| `S` | Speaker notes |
| `F` | Fullscreen |

## Running Locally

To view the slides locally:

```bash
cd docs/slides
python -m http.server 8080
# Open http://localhost:8080 in your browser
```

Or build the docs:

```bash
mkdocs serve
# Navigate to http://localhost:8000/slides/
```
