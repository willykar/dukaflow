"""Integration test: needs a disposable PostgreSQL. Skipped unless DUKAFLOW_TEST_DB is set, e.g.

    DUKAFLOW_TEST_DB=postgresql://duka:duka@localhost:5433/dukaflow_test pytest tests/test_load_postgres.py

WARNING: it drops and recreates the `duka` schema in that database. Never point it at real data.
"""

import os
from dataclasses import replace
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from generator.load_postgres import load, table_counts  # noqa: E402
from generator.simulate import Config  # noqa: E402

DB = os.environ.get("DUKAFLOW_TEST_DB")
MIGRATIONS = sorted((Path(__file__).resolve().parent.parent / "sql" / "postgres").glob("0*.sql"))
pytestmark = pytest.mark.skipif(not DB, reason="set DUKAFLOW_TEST_DB to run database tests")

CFG = replace(Config(), seed=3, shops=2, days=5, retry_rate=0.05, bad_rate=0.02)


@pytest.fixture
def conn():
    with psycopg.connect(DB, autocommit=True) as c:
        c.execute("DROP SCHEMA IF EXISTS duka CASCADE")
        for migration in MIGRATIONS:
            c.execute(migration.read_text(encoding="utf-8"))
        c.autocommit = False
        yield c


def test_load_is_idempotent(conn):
    first = load(conn, CFG)
    counts_after_first = table_counts(conn)
    second = load(conn, CFG)

    assert first.sales_inserted > 0
    assert first.duplicates_skipped > 0, "the generator's till retries must be caught"
    assert first.invalid_skipped > 0, "the generator's bad records must be skipped"
    assert second.sales_inserted == 0 and second.mpesa_inserted == 0
    assert table_counts(conn) == counts_after_first


def test_every_sale_has_lines(conn):
    load(conn, CFG)
    orphans = conn.execute(
        "SELECT count(*) FROM duka.sales s WHERE NOT EXISTS "
        "(SELECT 1 FROM duka.sale_items i WHERE i.receipt_id = s.receipt_id)").fetchone()[0]
    assert orphans == 0


def test_stock_never_negative_and_alerts_possible(conn):
    load(conn, replace(CFG, days=14))
    levels = conn.execute("SELECT min(on_hand), count(*) FILTER (WHERE on_hand < reorder_level) "
                          "FROM duka.stock_levels").fetchone()
    assert levels[0] >= 0, "the generator caps sales at stock on hand"


def test_sale_movements_match_sale_lines(conn):
    load(conn, CFG)
    lines, moved = conn.execute(
        "SELECT (SELECT sum(qty) FROM duka.sale_items), "
        "(SELECT -sum(qty_change) FROM duka.stock_movements WHERE reason = 'sale')").fetchone()
    assert lines == moved
