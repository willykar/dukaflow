# Operating DukaFlow

How to run the streaming layer, connect a shop's tills, watch it, and what happens when something breaks.
Every failure behaviour below was exercised on a live stack (see "Verified" in `architecture.md`).

## Running it

```bash
docker compose --profile app up -d --build   # Kafka, PostgreSQL, migrations, gateway, both consumers
docker compose --profile app ps              # all should be "Up" (Kafka and Postgres "healthy")
docker compose --profile app down            # stop (add -v to also delete the database volume)
```

| Service | Port | What it does |
|---|---|---|
| till-gateway | 8080 | `POST /v1/sales` from tills; `/healthz`, `/readyz`, `/metrics` |
| sales-consumer | 9101 | writes sales, lines and stock movements to PostgreSQL; `/metrics`, `/healthz` |
| stock-watch | 9102 | raises low-stock alerts; `/metrics`, `/healthz` |
| PostgreSQL | 5433 | database `dukaflow`, schema `duka` |
| Kafka | 9092 | topics `pos.sales` (6 partitions), `pos.sales.dlq` (1), `pos.alerts` (3) |

Migrations in `sql/postgres/` run automatically on every `up` (they are safe to re-run). Topics are created
automatically by the services on start, or explicitly with `go run ./cmd/admin topics`.

All settings are environment variables; see `.env.example`.

## Onboarding a shop

1. Add the shop and its tills to `duka.shops` and `duka.tills` (the dashboard will do this in phase 6).
2. Create a key for it (run from `go/`):
   ```bash
   go run ./cmd/admin create-key -shop 7 -label "Neema Duka, till 1"
   ```
   The key (`dk_...`) is printed **once**. Only its SHA-256 hash is stored, so it cannot be recovered;
   lose it and you create a new one.
3. Configure the till with the gateway URL and the key. A key can only post sales for its own shop.

Revoke a key: `go run ./cmd/admin revoke-key -key dk_...`. It stops working within `AUTH_CACHE_TTL` (1 minute).

## The till API

```
POST /v1/sales
Authorization: Bearer dk_...
Content-Type: application/json

{"receipt_id": "uuid made by the till", "shop_id": 1, "till_id": 11,
 "sold_at": "2026-11-19T09:30:00Z", "synced_at": "2026-11-19T09:30:02Z",
 "payment_method": "cash" | "mpesa", "customer_id": 3 | null,
 "items": [{"sku": "UNGA-2KG", "qty": 2, "unit_price_cents": 19500}]}
```

| Response | Meaning | Till should |
|---|---|---|
| 202 | Durably queued on Kafka | forget the sale |
| 400 | Invalid; it was dead-lettered for inspection | not retry; fix the till |
| 401 | Missing, unknown or revoked key | not retry; check the key |
| 403 | Key belongs to another shop | not retry |
| 503 | Could not queue right now | retry the **same** sale (same `receipt_id`) with backoff |

Retrying is always safe: every layer de-duplicates on `receipt_id`. An offline till simply posts its
backlog when it reconnects, with the original `sold_at`.

## Watching it

Scrape `/metrics` (Prometheus text format) on 8080, 9101 and 9102. The numbers that matter:

| Metric | Healthy | Investigate when |
|---|---|---|
| `dukaflow_gateway_sales_accepted_total` | rising during trading hours | flat while shops are open |
| `dukaflow_gateway_sales_rejected_total` | small | a jump: a till is sending bad data (check the DLQ) |
| `dukaflow_gateway_unauthorized_total` / `_forbidden_total` | ~0 | any sustained rate: misconfigured or hostile client |
| `dukaflow_gateway_publish_errors_total` | 0 | > 0: Kafka unreachable |
| `dukaflow_consumer_retries_total` | 0 | rising: PostgreSQL unreachable (consumption is paused, nothing is lost) |
| `dukaflow_consumer_dead_lettered_total` | 0 | > 0: a sale passed the gateway but the database refused it (e.g. unknown product) |
| `dukaflow_consumer_duplicates_total` | small | normal: till retries and redeliveries |
| `dukaflow_stock_alerts_opened_total` | some | – |

Consumer lag (how far behind the consumers are):

```bash
docker exec duka-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

Logs are JSON lines on stdout (`docker compose logs -f till-gateway`); every gateway response carries an
`X-Request-ID` that also appears in the logs.

## The dead-letter queue

```bash
go run ./cmd/admin dlq -max 50
```

Each entry shows why it failed (`dlq-reason`), where it came from (gateway, or topic/partition/offset), when,
and the original payload. Typical causes: a till bug (negative quantity, missing receipt id), a product the
shop sells that is not in the catalogue yet, a till assigned to the wrong shop. Fix the cause; the payload
can be re-posted to the gateway unchanged.

## When something breaks

| Failure | What happens | What you do |
|---|---|---|
| **PostgreSQL down** | Gateway keeps accepting sales for known keys (it uses the last good key lookup) and `/readyz` reports 503. Consumers pause and retry without committing. When the database returns they catch up. **No sale is lost or doubled.** | Restore the database. Watch `consumer_retries_total` stop rising and lag fall to 0. |
| **Kafka down** | Gateway answers 503; tills keep the sale and retry. Consumers reconnect automatically. | Restore Kafka. Tills drain their backlog by themselves. |
| **A consumer crashes or is redeployed** | Its uncommitted messages are redelivered to the group. Duplicates are skipped by `receipt_id`. | Nothing. |
| **Bad data from a till** | 400 to the till, payload in the DLQ, never in the database. | Read the DLQ, fix the till. |
| **Stolen key** | Can only post sales for that one shop. | Revoke it; issue a new one. |

## Scaling

- More throughput: raise `CONSUMER_WORKERS` (up to the 6 partitions of `pos.sales`), or run more
  sales-consumer containers in the same group; Kafka rebalances partitions between them.
- Ordering is per shop (the message key is `shop_id`), which is what stock tracking needs.
- The gateway is stateless apart from its key cache: run several behind a load balancer.
