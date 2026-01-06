# Presentation

An interactive slide deck covering polars-redis features and usage.

<a href="slides/index.html" class="md-button md-button--primary" target="_blank">
    Open Presentation
</a>

!!! note "Direct Link"
    If the button above doesn't work, you can access the slides directly at:
    [https://joshrotenberg.github.io/polars-redis/slides/](https://joshrotenberg.github.io/polars-redis/slides/)

## Topics Covered

- What is Polars? (quick primer)
- What is polars-redis?
- Scanning Redis Hashes and JSON
- Writing DataFrames to Redis
- Schema inference
- RediSearch integration (FT.SEARCH, FT.AGGREGATE)
- Performance features
- Configuration options
- Use case: Ephemeral Data Workbench
- Architecture overview
- Installation

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
