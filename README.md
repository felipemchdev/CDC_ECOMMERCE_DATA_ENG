# CDC_ECOMMERCE_DATA_ENG

## English

This repository implements a local, deterministic CDC (Change Data Capture) pipeline for an e-commerce domain using a Medallion architecture. It generates synthetic transactional events for `users`, `products`, `orders`, `order_items`, and `payments`, writes immutable raw events to Bronze, applies incremental current-state merges in Silver, and builds analytics marts in Gold.

The implementation targets production-style engineering concerns: deterministic generation (fixed seed), idempotent merge semantics, late-event handling by event time ordering, payload schema validation, and run-level observability metrics. The full stack runs locally with no cloud dependency.

Gold outputs are derived only from Silver and are reproducible. Delivered marts are: `daily_gmv`, `orders_by_status`, `refund_rate`, `top_products`, and `basic_retention`.

### Architecture

```text
            +------------------------------+
            | CDC Generator (deterministic)|
            +--------------+---------------+
                           |
                           v
                data/bronze/event_date=YYYY-MM-DD/
                     append-only parquet batches
                           |
                           v
                 Silver incremental merge
          (idempotent, event-time ordered, deletes)
                           |
                           v
                 data/silver/*.parquet
                           |
                           v
                 Gold marts from Silver
                 data/gold/*.parquet
```

### Temporal Coverage

- Simulation start date (inclusive): `2021-01-01`
- Earliest accepted `run`/`backfill` date: `2021-01-01`
- Dates before `2021-01-01` fail fast by design
- This guarantees a supported synthetic history window of at least 5 years when backfilling into 2026

5-year backfill example:

```bash
python -m cdc_ecommerce backfill --start 2021-01-01 --end 2026-01-01
```

### Repository Layout

```text
CDC_ECOMMERCE_DATA_ENG/
  src/cdc_ecommerce/
    ingestion/
    bronze/
    silver/
    gold/
    quality/
    utils/
    cli.py
    pipeline.py
  data/
    bronze/
    silver/
    gold/
    metrics/
  tests/
  .github/workflows/tests.yml
  pyproject.toml
  Makefile
```

### Run

```bash
python -m pip install -e ".[dev]"
python -m cdc_ecommerce run --date 2021-01-01
```

One-command path (after dependency install):

```bash
make run DATE=2021-01-01
```

Short backfill example:

```bash
python -m cdc_ecommerce backfill --start 2026-01-01 --end 2026-01-07
```

### Tests

```bash
python -m pytest
# or
make test
```

### Example Output Snippet

```json
{
  "run_date": "2026-01-01",
  "processed_events_count": 219,
  "runtime_seconds": 0.4474,
  "output_row_counts": {
    "silver": {
      "users": 12,
      "products": 40,
      "orders": 30,
      "order_items": 61,
      "payments": 23
    },
    "gold": {
      "daily_gmv": 1,
      "orders_by_status": 3,
      "refund_rate": 1,
      "top_products": 5,
      "basic_retention": 1
    }
  }
}
```

### Design Decisions and Trade-offs

- Idempotency: Silver tracks processed `event_id` and ignores already-applied events.
- Late events: each row stores `_last_event_ts`; older events cannot overwrite newer state.
- Bronze immutability: Bronze is append-only and partitioned by `event_date`.
- Merge semantics: entity-aware I/U/D handling with payload schema validation.
- Local-first stack: `pandas + duckdb + pydantic + typer + pytest`.
- Time horizon contract: bounded start date keeps deterministic growth and predictable local runtime.

### Limitations and Next Steps

- Volume anomaly detection is intentionally simple; seasonality-aware thresholds are a next step.
- Silver stores full current-state parquet tables; larger workloads would benefit from versioned table formats.
- Schema evolution is version-tracked but currently single-version in tests.
- Long-range backfills can be compute-heavy on small local machines.

## Portugues

Este repositorio implementa um pipeline local e deterministico de CDC para e-commerce com arquitetura Medallion. O fluxo gera eventos sinteticos, grava Bronze append-only, aplica merge incremental no Silver e monta marts no Gold.

O projeto foi estruturado para avaliacao tecnica de engenharia de dados: geracao deterministica por seed, merge idempotente, tratamento de eventos atrasados por `event_ts`, validacao de schema e metricas por execucao.

### Cobertura Temporal

- Data minima suportada: `2021-01-01`
- Datas anteriores falham por contrato
- Backfill de 5 anos suportado:

```bash
python -m cdc_ecommerce backfill --start 2021-01-01 --end 2026-01-01
```

### Como executar

```bash
python -m pip install -e ".[dev]"
python -m cdc_ecommerce run --date 2021-01-01
python -m cdc_ecommerce backfill --start 2026-01-01 --end 2026-01-07
```

### Como testar

```bash
python -m pytest
```
