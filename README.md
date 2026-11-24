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
| Ingestion API | Go (`net/http`) till gateway: per-shop API keys (hashed), request IDs, metrics |
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
├── requirements.txt           Python dependencies (Airflow runs in Docker)
├── docker-compose.yml         local Kafka + PostgreSQL
├── .env.example               every setting the project reads
├── docs/
│   ├── architecture.md        design, data flow, decisions and the reasons for them
│   ├── interview-map.md       each interview topic -> where it lives in this repo
│   └── operations.md          running it: onboarding a shop, the till API, metrics, failure modes
├── generator/                 synthetic shop data: sales, M-Pesa, late syncs, bad records
├── sql/postgres/              3NF schema + SQL drills
├── go/                        till-gateway, sales-consumer, stock-watch, admin (keys, DLQ)
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
docker compose --profile app up -d --build   # Kafka, PostgreSQL (:5433), migrations, gateway (:8080), consumers
python -m venv .venv && .venv/Scripts/activate   # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python -m generator.load_postgres --shops 3 --days 14             # shops, products, history and stock
cd go && go run ./cmd/admin create-key -shop 1 -label "demo till" && cd ..   # prints dk_... once
python -m generator.live --shop 1 --key dk_... --rate 5 --count 300          # live sales -> gateway
cd go && go run ./cmd/admin dlq                                  # what was rejected, and why
```

Then watch `http://localhost:8080/metrics`, `:9101/metrics` and `:9102/metrics`, or query PostgreSQL on
port 5433. Running it day to day: [docs/operations.md](docs/operations.md).

## Roadmap

| Phase | Scope | State |
|---|---|---|
| 1 | Data generator, 3NF PostgreSQL, SQL analytics | Done: idempotent loader; index cut a till's daily query from 7.0 ms to 0.10 ms |
| 2 | Go till gateway, Kafka, idempotent consumer, dead-letter topic, stock alerts | Done: 600 live sales with a consumer restart -> stored exactly once; survives PostgreSQL and Kafka outages without losing a sale |
| 3 | Data lake + PySpark | Next |
| 4 | Snowflake + dbt + Airflow, late-arriving sales | Planned |
| 5 | Gemini: M-Pesa statements, supplier invoices, "ask your shop" | Planned |
| 6 | FastAPI + React dashboard | Planned |
| 7 | CI, deployment, live demo | Planned |

Measurements and design decisions: [docs/architecture.md](docs/architecture.md).

## Data and privacy

All data is synthetic, produced by `generator/`. No real shop, customer or M-Pesa data is used. If you
ever load real statements, phone numbers are hashed on ingestion and never stored in plain text.
