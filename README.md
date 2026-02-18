# CDC_ECOMMERCE_DATA_ENG

## English

This repository implements a local, deterministic CDC (Change Data Capture) pipeline for an e-commerce domain using a Medallion architecture. It simulates transactional changes for `users`, `products`, `orders`, `order_items`, and `payments`, writes immutable event logs to Bronze, incrementally merges those events into current-state Silver tables, and materializes analytics-ready Gold marts.

The project is designed as a production-style portfolio baseline: deterministic synthetic generation with a fixed seed, idempotent incremental merge logic, late-event protection by `event_ts`, schema validation for CDC payloads, and run-level observability metrics. It can run fully offline on a local machine, without cloud services.

Gold marts are built strictly from Silver current-state tables, with reproducible outputs for: `daily_gmv`, `orders_by_status`, `refund_rate`, `top_products`, and `basic_retention`. Quality gates are applied on every run (row-count checks, referential integrity, non-negative monetary values, and a basic volume anomaly check).

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
python -m cdc_ecommerce run --date 2026-01-01
```

One-command path (after dependency install):

```bash
make run DATE=2026-01-01
```

Backfill example:

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
  "processed_events_count": 248,
  "runtime_seconds": 0.4123,
  "output_row_counts": {
    "silver": {"users": 14, "products": 42, "orders": 30, "order_items": 61, "payments": 23},
    "gold": {"daily_gmv": 1, "orders_by_status": 4, "refund_rate": 1, "top_products": 5, "basic_retention": 1}
  }
}
```

### Design Decisions and Trade-offs

- Idempotency: Silver tracks processed `event_id` values and ignores duplicates on reprocessing.
- Late events: each primary key row stores `_last_event_ts`; older arriving events do not overwrite newer state.
- Partitions: Bronze is partitioned by `event_date`, append-only, and never mutated.
- Merge approach: entity-specific upsert/delete semantics with schema validation before apply.
- Local-first stack: `pandas + duckdb + pydantic + typer + pytest` for minimal setup and reproducibility.

### Limitations and Next Steps

- Current volume anomaly check is intentionally simple; add seasonality-aware thresholds for larger workloads.
- Silver currently stores full current-state parquet tables; larger datasets would benefit from table formats with snapshot/version metadata.
- Add data contracts and schema evolution migration tests for future `schema_version` increments.
- Add benchmark scenarios and load tests for higher event throughput.

## Português

Este repositório implementa um pipeline local e determinístico de CDC (Change Data Capture) para um domínio de e-commerce com arquitetura Medallion. O projeto simula mudanças transacionais para `users`, `products`, `orders`, `order_items` e `payments`, grava logs imutáveis no Bronze, aplica merge incremental para estado atual no Silver e gera marts analíticos no Gold.

O objetivo é refletir um padrão de engenharia de dados de produção: geração sintética determinística com seed fixa, merge incremental idempotente, proteção contra eventos atrasados com base em `event_ts`, validação de schema dos payloads CDC e métricas de observabilidade por execução. Tudo roda localmente, sem dependência de cloud.

Os marts Gold são derivados exclusivamente das tabelas Silver de estado atual: `daily_gmv`, `orders_by_status`, `refund_rate`, `top_products` e `basic_retention`. Em cada execução, o pipeline aplica checagens de qualidade (contagem mínima, integridade referencial, valores monetários não negativos e verificação simples de anomalia de volume).

### Como executar

```bash
python -m pip install -e ".[dev]"
python -m cdc_ecommerce run --date 2026-01-01
python -m cdc_ecommerce backfill --start 2026-01-01 --end 2026-01-07
```

### Como testar

```bash
python -m pytest
```
