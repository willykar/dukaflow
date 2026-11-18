"""Load generated data into the 3NF schema (phase 1.3).

TODO:
- read POSTGRES_URL from .env (python-dotenv); connect with psycopg
- seed shops, tills, categories, products, product_prices from generator.simulate.PRODUCTS
- insert sales + sale_items in one transaction per sale, ON CONFLICT (receipt_id) DO NOTHING
- skip records where generator.simulate.is_valid() is False and count them
- run twice; assert row counts are unchanged (idempotency)
"""
