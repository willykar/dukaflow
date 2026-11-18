"""PySpark: lake raw (by sync date) -> curated sale_items Parquet (by year/month of sold_at). Phase 3.

TODO:
- SparkSession; read LAKE_ROOT/raw/sales with an EXPLICIT schema (no inference on big data)
- explode items into one row per sale line; drop duplicate (receipt_id, line) pairs
- add year/month from sold_at (event time) and write partitionBy("year", "month") to LAKE_ROOT/curated/sale_items
- join products/shops with broadcast(...); save df.explain() with and without it to docs/
- watch http://localhost:4040 while it runs; screenshot the DAG
"""
