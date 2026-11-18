-- DukaFlow transactional schema (PostgreSQL, 3NF).
-- sold_at = event time (till); synced_at = processing time (arrival). Offline tills make them days apart.

CREATE SCHEMA IF NOT EXISTS duka;
SET search_path TO duka;

CREATE TABLE IF NOT EXISTS shops (
    shop_id         int PRIMARY KEY,
    name            text NOT NULL,
    town            text NOT NULL
);

CREATE TABLE IF NOT EXISTS tills (
    till_id         int PRIMARY KEY,
    shop_id         int NOT NULL REFERENCES shops
);

CREATE TABLE IF NOT EXISTS categories (
    category_id     smallint PRIMARY KEY,
    name            text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS products (
    sku             text PRIMARY KEY,
    name            text NOT NULL,
    category_id     smallint NOT NULL REFERENCES categories,
    reorder_level   int NOT NULL DEFAULT 10 CHECK (reorder_level >= 0)
);

-- A price changes over time, so it is its own table, not a column on products.
CREATE TABLE IF NOT EXISTS product_prices (
    sku             text NOT NULL REFERENCES products,
    valid_from      timestamptz NOT NULL,
    price_cents     int NOT NULL CHECK (price_cents > 0),
    PRIMARY KEY (sku, valid_from)
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    display_name    text NOT NULL
);

CREATE TABLE IF NOT EXISTS sales (
    receipt_id      uuid PRIMARY KEY,                       -- generated on the till: the idempotency key
    till_id         int NOT NULL REFERENCES tills,
    sold_at         timestamptz NOT NULL,
    synced_at       timestamptz NOT NULL,
    payment_method  text NOT NULL CHECK (payment_method IN ('cash', 'mpesa')),
    customer_id     bigint REFERENCES customers,            -- NULL for walk-in customers
    CHECK (synced_at >= sold_at - interval '5 minutes')     -- allow small till clock skew
);

CREATE TABLE IF NOT EXISTS sale_items (
    receipt_id          uuid NOT NULL REFERENCES sales ON DELETE CASCADE,
    line_no             smallint NOT NULL,
    sku                 text NOT NULL REFERENCES products,
    qty                 int NOT NULL CHECK (qty > 0),
    unit_price_cents    int NOT NULL CHECK (unit_price_cents >= 0),  -- price at time of sale
    PRIMARY KEY (receipt_id, line_no)
);

CREATE TABLE IF NOT EXISTS mpesa_transactions (
    mpesa_ref           text PRIMARY KEY CHECK (mpesa_ref ~ '^[A-Z0-9]{10}$'),
    shop_id             int NOT NULL REFERENCES shops,
    amount_cents        int NOT NULL CHECK (amount_cents > 0),
    paid_at             timestamptz NOT NULL,
    payer_phone_hash    char(64)                            -- sha256; never store the raw number
);

CREATE TABLE IF NOT EXISTS stock_movements (
    movement_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shop_id         int NOT NULL REFERENCES shops,
    sku             text NOT NULL REFERENCES products,
    moved_at        timestamptz NOT NULL,
    qty_change      int NOT NULL CHECK (qty_change <> 0),
    reason          text NOT NULL CHECK (reason IN ('delivery', 'sale', 'adjustment'))
);

-- Indexes. EXPLAIN ANALYZE before and after each; record both plans in docs/architecture.md.
CREATE INDEX IF NOT EXISTS ix_sales_till_sold_at   ON sales (till_id, sold_at);
CREATE INDEX IF NOT EXISTS ix_sales_synced_at      ON sales (synced_at);
CREATE INDEX IF NOT EXISTS ix_mpesa_shop_paid_at   ON mpesa_transactions (shop_id, paid_at);
CREATE INDEX IF NOT EXISTS ix_stock_shop_sku       ON stock_movements (shop_id, sku, moved_at);
