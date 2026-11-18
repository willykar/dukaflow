# Architecture

## The problem, in one paragraph

A duka owner needs three answers every day: what is selling, what is about to run out, and whether every
M-Pesa payment matches a sale. Tills sometimes work offline and sync days later, payments arrive through a
separate channel, and some records are simply wrong. The system has to be fast for today's numbers and
correct for history, at the same time.

## Lambda architecture

- **Speed layer** (Go + Kafka + PostgreSQL): a sale is visible on the dashboard seconds after the till sends
  it. Low-stock alerts are computed on the stream.
- **Batch layer** (data lake + PySpark + Snowflake + dbt, nightly in Airflow): recomputes history from all
  raw data, including sales that arrived days late, so trends and reconciliation are complete and correct.
- **Serving layer** (FastAPI + React): today's tiles read the speed layer; trends, stock cover and
  reconciliation read the dbt marts.

## Data flow

| Step | From | To | Tool | Cadence |
|---|---|---|---|---|
| 1 | till / generator | `POST /v1/sales` | Go till-gateway | continuous |
| 2 | gateway | Kafka `pos.sales` (key `shop_id`) | Go | continuous |
| 3 | `pos.sales` | PostgreSQL `sales`, `sale_items` | Go sales-consumer (group `sales-writer`) | continuous |
| 4 | `pos.sales` | Kafka `pos.alerts` | Go stock-watch (group `stock-watch`) | continuous |
| 5 | raw sales, M-Pesa | lake `raw/` partitioned by sync date | generator / extract | nightly |
| 6 | lake `raw/` | lake `curated/` Parquet by year/month of `sold_at` | PySpark | nightly |
| 7 | lake `curated/` | Snowflake `RAW` | internal stage `PUT` + `COPY INTO` + `MERGE` | nightly |
| 8 | Snowflake `RAW` | `ANALYTICS` marts, `QUARANTINE` | dbt | nightly |
| 9 | statements, invoices | validated rows | Gemini + Pydantic | on upload |
| 10 | marts + Postgres + `pos.alerts` | dashboard | FastAPI + React | on request / live |

## Design decisions (each one is an interview answer; fill in your evidence)

- **3NF in Postgres, denormalised marts in Snowflake.** Many small writes favour 3NF (a price lives in one
  place); the dashboard reads favour wide, pre-joined marts.
- **Event time vs processing time.** `sold_at` (event) and `synced_at` (processing) are both stored. Raw is
  partitioned by sync date (what each nightly run must pick up); curated by `sold_at` year/month (what
  analysts filter on).
- **Idempotency by receipt.** The till generates `receipt_id`; every layer de-duplicates on it, so retries
  and replays never double-count.
- **At-least-once + idempotent writes.** The consumer commits its Kafka offset only after the Postgres
  transaction commits; the insert is `ON CONFLICT (receipt_id) DO NOTHING`.
- **Kafka key = `shop_id`.** One shop's sales stay in order in one partition, which stock tracking needs.
- **Two consumer groups on one topic.** `sales-writer` and `stock-watch` each receive every sale independently.
- **Late data through MERGE + lookback.** A late till changes days already reported; marts are rebuilt for
  the `sold_at` dates touched.
- **Quarantine, don't drop.** Bad records go to a DLQ (stream) or `QUARANTINE` schema (warehouse), where they
  can be counted and fixed.
- **No cloud bill required.** Local lake with a bucket-style layout, Snowflake internal stage, self-hosted Kafka.
- **The LLM never writes SQL.** It chooses from a fixed catalogue of parameterised queries.
- **Phone numbers are hashed** on ingestion (Kenya Data Protection Act).

## Kafka topics

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic pos.sales --partitions 6 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic pos.sales.dlq --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic pos.alerts --partitions 3 --replication-factor 1
```

## Evidence (paste as you produce it)

- `EXPLAIN ANALYZE` before and after the index (phase 1)
- Restart-without-duplicates and DLQ proof (phase 2)
- Spark plans with and without `broadcast()`, Spark UI DAG (phase 3)
- Late batch changing an old day's total; dbt lineage graph (phase 4)
