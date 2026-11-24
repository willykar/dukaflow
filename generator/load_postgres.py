"""Load generated data into the 3NF schema, idempotently.

    python -m generator.load_postgres --shops 5 --days 30

Run it twice: the second run inserts nothing, because every sale is keyed on its till-generated
receipt_id and every insert is ON CONFLICT DO NOTHING. Invalid records (the generator injects some on
purpose) are skipped and counted here; in the streaming path they go to the dead-letter topic instead.
"""

import argparse
import os
from dataclasses import dataclass, replace

import psycopg
from dotenv import load_dotenv

from generator.simulate import (CUSTOMERS, PAR_LEVEL, PRODUCTS, REORDER_LEVEL, Config, categories, is_valid,
                                shops, simulate, till_ids)


@dataclass
class LoadReport:
    sales_inserted: int = 0
    duplicates_skipped: int = 0
    invalid_skipped: int = 0
    items_inserted: int = 0
    mpesa_inserted: int = 0
    deliveries_inserted: int = 0


def seed_reference(conn, cfg):
    """Shops, tills, categories, products, starting prices and loyalty customers."""
    cat_ids = {name: cid for cid, name in categories()}
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO duka.shops (shop_id, name, town) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            shops(cfg.shops))
        cur.executemany(
            "INSERT INTO duka.tills (till_id, shop_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            [(t, sid) for sid, _, _ in shops(cfg.shops) for t in till_ids(sid)])
        cur.executemany(
            "INSERT INTO duka.categories (category_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            categories())
        cur.executemany(
            "INSERT INTO duka.products (sku, name, category_id, reorder_level, par_level) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            [(sku, name, cat_ids[cat], REORDER_LEVEL, PAR_LEVEL) for sku, name, cat, _ in PRODUCTS])
        cur.executemany(
            "INSERT INTO duka.product_prices (sku, valid_from, price_cents) VALUES (%s, %s, %s) "
            "ON CONFLICT DO NOTHING",
            [(sku, cfg.start, price) for sku, _, _, price in PRODUCTS])
        cur.executemany(
            "INSERT INTO duka.customers (customer_id, display_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            CUSTOMERS)


CHUNK = 5_000  # sales per transaction

_TEMP_TABLES = """
CREATE TEMP TABLE IF NOT EXISTS tmp_sales (receipt_id uuid, till_id int, sold_at timestamptz,
    synced_at timestamptz, payment_method text, customer_id int, shop_id int);
CREATE TEMP TABLE IF NOT EXISTS tmp_items (receipt_id uuid, line_no smallint, sku text, qty int,
    unit_price_cents int);
CREATE TEMP TABLE IF NOT EXISTS tmp_new (receipt_id uuid);
TRUNCATE tmp_sales, tmp_items, tmp_new;
"""


def load_sales(conn, sales, report):
    """Bulk, set-based and idempotent.

    Per chunk, in ONE transaction: COPY the sales and lines into temp tables, insert the sales with
    ON CONFLICT (receipt_id) DO NOTHING, then insert lines and stock movements only for the receipts that
    were actually new. A sale and its lines still land together or not at all, but each chunk costs a
    handful of round trips instead of several per sale, which matters a lot over a network or Docker.
    """
    seen, valid = set(), []
    for sale in sales:
        if not is_valid(sale):
            report.invalid_skipped += 1
        elif sale["receipt_id"] in seen:
            report.duplicates_skipped += 1  # a till retry inside this batch
        else:
            seen.add(sale["receipt_id"])
            valid.append(sale)
    for i in range(0, len(valid), CHUNK):
        _load_chunk(conn, valid[i:i + CHUNK], report)


def _load_chunk(conn, chunk, report):
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(_TEMP_TABLES)
        with cur.copy("COPY tmp_sales FROM STDIN") as cp:
            for s in chunk:
                cp.write_row((s["receipt_id"], s["till_id"], s["sold_at"], s["synced_at"], s["payment_method"],
                              s["customer_id"], s["shop_id"]))
        with cur.copy("COPY tmp_items FROM STDIN") as cp:
            for s in chunk:
                for n, i in enumerate(s["items"], start=1):
                    cp.write_row((s["receipt_id"], n, i["sku"], i["qty"], i["unit_price_cents"]))
        cur.execute("""
            WITH ins AS (
                INSERT INTO duka.sales (receipt_id, till_id, sold_at, synced_at, payment_method, customer_id)
                SELECT receipt_id, till_id, sold_at, synced_at, payment_method, customer_id FROM tmp_sales
                ON CONFLICT (receipt_id) DO NOTHING
                RETURNING receipt_id)
            INSERT INTO tmp_new SELECT receipt_id FROM ins""")
        inserted = cur.rowcount
        cur.execute("""
            INSERT INTO duka.sale_items (receipt_id, line_no, sku, qty, unit_price_cents)
            SELECT i.receipt_id, i.line_no, i.sku, i.qty, i.unit_price_cents
            FROM tmp_items i JOIN tmp_new USING (receipt_id)""")
        report.items_inserted += cur.rowcount
        # Same rows the Go sales-consumer writes: stock goes down with the sale, in the same transaction.
        cur.execute("""
            INSERT INTO duka.stock_movements (shop_id, sku, moved_at, qty_change, reason, receipt_id)
            SELECT s.shop_id, i.sku, s.sold_at, -i.qty, 'sale', i.receipt_id
            FROM tmp_items i JOIN tmp_new USING (receipt_id) JOIN tmp_sales s USING (receipt_id)""")
    report.sales_inserted += inserted
    report.duplicates_skipped += len(chunk) - inserted  # already in the database: an earlier run


def load_mpesa(conn, transactions, report):
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_mpesa (mpesa_ref text, shop_id int, amount_cents int, "
                    "paid_at timestamptz); TRUNCATE tmp_mpesa")
        with cur.copy("COPY tmp_mpesa FROM STDIN") as cp:
            for t in transactions:
                cp.write_row((t["mpesa_ref"], t["shop_id"], t["amount_cents"], t["paid_at"]))
        cur.execute("INSERT INTO duka.mpesa_transactions (mpesa_ref, shop_id, amount_cents, paid_at) "
                    "SELECT DISTINCT ON (mpesa_ref) * FROM tmp_mpesa ON CONFLICT (mpesa_ref) DO NOTHING")
        report.mpesa_inserted += cur.rowcount


def load_deliveries(conn, deliveries, report):
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_deliveries (shop_id int, sku text, moved_at timestamptz, "
                    "qty int); TRUNCATE tmp_deliveries")
        with cur.copy("COPY tmp_deliveries FROM STDIN") as cp:
            for d in deliveries:
                cp.write_row((d["shop_id"], d["sku"], d["delivered_at"], d["qty"]))
        cur.execute("INSERT INTO duka.stock_movements (shop_id, sku, moved_at, qty_change, reason) "
                    "SELECT shop_id, sku, moved_at, qty, 'delivery' FROM tmp_deliveries "
                    "ON CONFLICT (shop_id, sku, moved_at) WHERE reason = 'delivery' DO NOTHING")
        report.deliveries_inserted += cur.rowcount


def load(conn, cfg):
    data = simulate(cfg)
    report = LoadReport()
    with conn.transaction():
        seed_reference(conn, cfg)
    load_deliveries(conn, data["deliveries"], report)
    load_sales(conn, data["sales"], report)
    load_mpesa(conn, data["mpesa"], report)
    return report


def table_counts(conn):
    with conn.cursor() as cur:
        out = {}
        for table in ("sales", "sale_items", "mpesa_transactions", "stock_movements", "shops", "tills",
                      "products"):
            cur.execute(f"SELECT count(*) FROM duka.{table}")  # fixed names, not user input
            out[table] = cur.fetchone()[0]
        return out


def main(argv=None):
    p = argparse.ArgumentParser(description="Load generated duka data into PostgreSQL")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--shops", type=int, default=5)
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--db", help="connection URL (default: POSTGRES_URL from .env)")
    args = p.parse_args(argv)

    load_dotenv()
    url = args.db or os.environ.get("POSTGRES_URL")
    if not url:
        raise SystemExit("POSTGRES_URL is not set (copy .env.example to .env)")

    cfg = replace(Config(), seed=args.seed, shops=args.shops, days=args.days)
    with psycopg.connect(url) as conn:
        report = load(conn, cfg)
        print(report)
        print(table_counts(conn))


if __name__ == "__main__":
    main()
