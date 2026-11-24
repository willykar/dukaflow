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

The services create these on start (`bus.EnsureTopics`); the manual equivalent is:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic pos.sales --partitions 6 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic pos.sales.dlq --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic pos.alerts --partitions 3 --replication-factor 1
```

## Evidence (paste as you produce it)

### Phase 1: index on `sales (till_id, sold_at)`

Data: `python -m generator.load_postgres --shops 10 --days 120` -> 94,159 sales, 234,996 sale lines,
57,200 M-Pesa rows (1,853 till retries and 1,049 invalid records skipped). PostgreSQL 18, laptop.
Query: one till's sales for one day (`sql/postgres/queries/indexing.sql`), 60 matching rows.

| | Plan | Pages read | Execution time |
|---|---|---|---|
| Without index | `Seq Scan on sales` | 908 | 7.0 ms |
| With index | `Bitmap Index Scan on ix_sales_till_sold_at` -> `Bitmap Heap Scan` | 6 | 0.10 ms |

About 70x faster and 150x fewer pages, for a query the dashboard's "Today" tile runs constantly. The gap
grows with the table: a sequential scan reads every sale ever made; the index reads only that till's day.

### Phase 1: M-Pesa reconciliation on the same data (`sql/postgres/queries/joins.sql`)

1,145 M-Pesa sales with no payment and 1,938 payments with no sale, both in line with the generator's 2%
unmatched rate. An INNER JOIN would have reported zero problems.

### Phase 2: the streaming layer, verified on a live Docker stack (2026-11-19)

Setup: `docker compose --profile app up` (Kafka 3.8, PostgreSQL 16, gateway, sales-consumer with 3 workers,
stock-watch), 3 shops seeded with 14 days of history and stock, one API key for shop 1.

| Test | Result |
|---|---|
| 600 live sales (`generator.live`), 3% invalid, 5% late, 5% re-sent, **sales-consumer restarted mid-stream** | 591 accepted (202) = **591 new rows in PostgreSQL**; 29 re-sends accepted but skipped as duplicates; 9 invalid -> 400 and **9 in the DLQ** with reasons; 34 late syncs; every sale line has its stock movement |
| Consumer lag after the run | 0 for both groups (`sales-writer`, `stock-watch`) at offset 620 |
| Partitioning | all of shop 1's sales on one partition (key = `shop_id`), so they stay in order |
| Low-stock alerts | 12 opened in PostgreSQL = 12 messages on `pos.alerts`, each once |
| No key / unknown key / shop 1's key posting a shop 2 sale | 401 / 401 / 403 |
| **PostgreSQL stopped**, 150 sales posted | gateway `/readyz` 503 but all **150 accepted** (cached key lookups); consumer paused; after restart the database had exactly before + 150 |
| **Kafka stopped**, one sale posted | 503 after ~8 s (till keeps it); same sale retried after restart -> 202, stored once |

Bulk seeding: 10 shops x 120 days (92,517 sales, 228,791 lines, 56,073 M-Pesa rows) into the Docker
PostgreSQL in 57 s with COPY + set-based inserts, versus ~10 sales/s row by row over Docker's port
forwarding; a second run inserted nothing and left every count unchanged.

### Still to add
- Spark plans with and without `broadcast()`, Spark UI DAG (phase 3)
- Late batch changing an old day's total; dbt lineage graph (phase 4)
