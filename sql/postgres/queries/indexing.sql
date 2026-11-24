-- Indexing drill: run with psql against a database loaded with a few months of data
-- (python -m generator.load_postgres --shops 10 --days 120) and paste both plans into docs/architecture.md.
SET search_path TO duka;

-- The question: one till's sales for one day, which the "Today" tile asks constantly.
-- 1. Without the index.
DROP INDEX IF EXISTS ix_sales_till_sold_at;
ANALYZE sales;
EXPLAIN (ANALYZE, BUFFERS)
SELECT receipt_id, sold_at
FROM sales
WHERE till_id = 11
  AND sold_at >= '2026-02-10' AND sold_at < '2026-02-11';

-- 2. With a composite B-tree index: equality column first (till_id), range column second (sold_at).
CREATE INDEX ix_sales_till_sold_at ON sales (till_id, sold_at);
ANALYZE sales;
EXPLAIN (ANALYZE, BUFFERS)
SELECT receipt_id, sold_at
FROM sales
WHERE till_id = 11
  AND sold_at >= '2026-02-10' AND sold_at < '2026-02-11';

-- What to look for: "Seq Scan on sales" (reads every row, then filters) becomes a scan that uses
-- ix_sales_till_sold_at. For a few dozen matching rows Postgres often picks a "Bitmap Index Scan" + "Bitmap
-- Heap Scan" (collect matching row locations from the index, then read those pages in physical order) rather
-- than a plain "Index Scan"; both mean the index is doing the work. Compare "Buffers" and "Execution Time".
-- Column order matters: an index on (sold_at, till_id) would have to scan the whole day for every till.
