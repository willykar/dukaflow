# Interview map

Each topic you may be asked about, and where in this repo your own answer lives.

## Round 1: SQL and database design
| Topic | Where |
|---|---|
| Normalisation (3NF) vs denormalisation | `sql/postgres/001_schema.sql`, `docs/architecture.md`, `dbt/models/marts/` |
| Indexing, `EXPLAIN ANALYZE` | indexes in `001_schema.sql`, plans in `docs/architecture.md` |
| Inner / left / full outer joins | `sql/postgres/queries/joins.sql` (M-Pesa reconciliation) |
| `ROW_NUMBER`, `RANK`, rolling windows | `sql/postgres/queries/window_functions.sql` |
| `WHERE` vs `HAVING` | `sql/postgres/queries/aggregations.sql` |
| `COALESCE`, `NULLIF` | `sql/postgres/queries/null_handling.sql` |

## Round 2: data engineering
| Question area | Where |
|---|---|
| Pipeline design, orchestration (Q1, 2, 6) | `dags/`, `docs/architecture.md` |
| Spark architecture, partitioning, shuffles, lazy evaluation (Q3–4) | `spark/jobs/build_sales.py`, plans in `docs/` |
| Batch vs real-time (Q5) | `go/` speed layer vs `spark/` + `dags/` batch layer |
| Kafka partitions, keys, consumer groups | `go/cmd/sales-consumer`, `go/cmd/stock-watch` |
| At-least-once vs exactly-once | `go/cmd/sales-consumer`, `go/internal/store` |
| Dead-letter queue | `go/internal/gateway`, `pos.sales.dlq` |
| Late-arriving data, event vs processing time | `generator/simulate.py`, `snowflake/setup.sql`, phase 4.2 |
| Data quality, quarantine | `dbt/` tests and quarantine models |

## Hiring manager round
| Topic | Where |
|---|---|
| Trustworthy LLM output (Pydantic) | `agent/schemas.py` |
| AI that answers data questions safely | `agent/` "ask your shop", `api/main.py` `/ask` |
| Business value | the dashboard, the demo video, README |
