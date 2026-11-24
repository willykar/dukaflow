-- Migration 002: what the streaming layer needs to run as a product.
--   * API keys, so each shop's tills can only post sales for that shop
--   * stock movements linked to the sale that caused them, and a live stock view
--   * stock alerts that open once and close once (never spam the owner)
-- Apply after 001_schema.sql. Safe to run more than once.

SET search_path TO duka;

-- One key per till or per shop. Only the SHA-256 of the key is stored; the key itself is shown once
-- when created (go run ./cmd/admin create-key) and cannot be recovered from the database.
CREATE TABLE IF NOT EXISTS api_keys (
    key_hash        char(64) PRIMARY KEY,
    shop_id         int NOT NULL REFERENCES shops,
    label           text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    revoked_at      timestamptz
);

-- A sale's stock movements reference it, so they are written (and de-duplicated) with the sale.
ALTER TABLE stock_movements ADD COLUMN IF NOT EXISTS receipt_id uuid REFERENCES sales ON DELETE CASCADE;

-- Stock a shop wants to hold after a delivery; reorder_level (on products) triggers the alert.
ALTER TABLE products ADD COLUMN IF NOT EXISTS par_level int NOT NULL DEFAULT 250 CHECK (par_level > 0);

-- Current stock = every delivery, sale and adjustment added up.
CREATE OR REPLACE VIEW stock_levels AS
SELECT m.shop_id, m.sku, SUM(m.qty_change)::int AS on_hand, p.reorder_level, p.par_level
FROM stock_movements m
JOIN products p USING (sku)
GROUP BY m.shop_id, m.sku, p.reorder_level, p.par_level;

-- An alert is OPEN until stock recovers above the reorder level. The partial unique index allows
-- at most one open alert per shop and product, which is what makes alerting idempotent.
CREATE TABLE IF NOT EXISTS stock_alerts (
    alert_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shop_id         int NOT NULL REFERENCES shops,
    sku             text NOT NULL REFERENCES products,
    on_hand         int NOT NULL,
    reorder_level   int NOT NULL,
    opened_at       timestamptz NOT NULL DEFAULT now(),
    cleared_at      timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_stock_alerts_open
    ON stock_alerts (shop_id, sku) WHERE cleared_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_stock_receipt ON stock_movements (receipt_id);

-- A delivery is identified by shop, product and time, so reloading the same delivery is a no-op.
CREATE UNIQUE INDEX IF NOT EXISTS ux_stock_delivery
    ON stock_movements (shop_id, sku, moved_at) WHERE reason = 'delivery';
