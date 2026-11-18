# DukaFlow

**Real-time sales, stock and M-Pesa reconciliation for small shops.**

Kenya's *dukas* (neighbourhood shops) sell through a till, get paid by cash and M-Pesa, and restock from
suppliers on paper invoices. Owners rarely know, on any given day, what is selling, what is about to run
out, or whether every M-Pesa payment matches a sale. DukaFlow answers those three questions on a live
dashboard, and lets an owner ask in plain English: *"What sold best last week?"*

It is also a complete data engineering system, built as a **Lambda architecture**:

```
                        ┌───────────────────── SPEED LAYER (seconds) ─────────────────────┐
 Tills ──HTTP──► Go till-gateway ──► Kafka pos.sales ──► Go sales-consumer ──► PostgreSQL (3NF)
 (offline tills       │                     │                (idempotent on receipt_id)      │
  sync days late)     │                     └──► pos.sales.dlq (bad payloads)               │
                      │                                                                      ▼
                      └─────────────► Go stock-watch ──► Kafka pos.alerts ──────────►  FastAPI
                                                                                     (REST + live
 Data lake raw ──► PySpark ──► Snowflake (MERGE late sales) ──► dbt marts ─────────►  events)
 (partitioned)    └───────────────── BATCH LAYER (nightly, Airflow) ───────────────┘      │
                                                                                          ▼
 M-Pesa statements / supplier invoices ──► Gemini + Pydantic (strict JSON) ──►    React dashboard
                                                                                 + "Ask your shop"
```

## What the dashboard shows

- **Today, live**: sales, transactions and basket size per shop, updating as tills sync.
- **Stock cover**: which products run out in the next few days at the current sales rate.
- **M-Pesa reconciliation**: payments with no matching sale, and sales marked M-Pesa with no payment.
- **Trends**: top products by week, slow movers, sales by hour and day of week.
- **Ask your shop**: a plain-English question becomes one of a fixed set of safe queries, with an answer and a chart.
- **Late data, visible**: how many sales arrived late from offline tills, and how far back they reached.

## Tech stack

| Layer | Tools |
|---|---|
| Data source | Python generator simulating shops, tills, M-Pesa, offline syncs and bad records |
| Ingestion API | Go (`net/http`) till gateway |
| Streaming | Apache Kafka (KRaft), Go consumer groups, dead-letter topic |
| Transactional store | PostgreSQL (3NF), indexes checked with `EXPLAIN ANALYZE` |
| Data lake | Local folder with bucket-style `raw/` and `curated/` zones (swappable for S3, GCS or R2) |
| Batch | PySpark, partitioned by year and month |
| Warehouse | Snowflake (internal stage, `MERGE`), dbt (staging, marts, tests, quarantine) |
| Orchestration | Apache Airflow |
| AI | Gemini with Pydantic-validated output (M-Pesa statements, invoices, questions) |
| Product | FastAPI backend, React + TypeScript dashboard |
| Quality | pytest, Go tests, dbt tests, GitHub Actions |

No AWS account and no paid services are needed to run it.

## Repository layout

```
dukaflow/
├── README.md
├── IMPLEMENTATION.md          step-by-step build guide, start to finish
├── requirements.txt           Python dependencies (Airflow runs in Docker)
├── docker-compose.yml         local Kafka + PostgreSQL
├── .env.example               every setting the project reads
├── docs/
│   ├── architecture.md        design, data flow, decisions and the reasons for them
│   └── interview-map.md       each interview topic -> where it lives in this repo
├── generator/                 synthetic shop data: sales, M-Pesa, late syncs, bad records
├── sql/postgres/              3NF schema + SQL drills
├── go/                        till-gateway, sales-consumer, stock-watch
├── spark/jobs/                lake raw -> curated (Parquet)
├── snowflake/                 warehouse setup, internal stage, MERGE
├── dbt/                       staging, marts, tests
├── dags/                      Airflow DAGs
├── agent/                     Gemini parsing and "ask your shop"
├── api/                       FastAPI backend for the dashboard
├── web/                       React + TypeScript dashboard (created in phase 6)
└── tests/                     pytest suite
```

## Quick start

```bash
cp .env.example .env
docker compose up -d                         # Kafka :9092, PostgreSQL :5432
python -m venv .venv && .venv/Scripts/activate   # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
psql "$POSTGRES_URL" -f sql/postgres/001_schema.sql
python -m generator.cli --shops 5 --days 30 --out data/lake/raw   # history for the batch layer
cd go && go run ./cmd/till-gateway           # live tills post here
```

The full build, from an empty folder to a deployed demo, is in [IMPLEMENTATION.md](IMPLEMENTATION.md).

## Status

Scaffolded. Progress is tracked phase by phase in [IMPLEMENTATION.md](IMPLEMENTATION.md).

## Data and privacy

All data is synthetic, produced by `generator/`. No real shop, customer or M-Pesa data is used. If you
ever load real statements, phone numbers are hashed on ingestion and never stored in plain text.
