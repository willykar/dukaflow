SET search_path TO duka;

-- 1. Latest price per product (ROW_NUMBER keeps exactly one row per product).
WITH ranked AS (
    SELECT sku, price_cents, valid_from,
           ROW_NUMBER() OVER (PARTITION BY sku ORDER BY valid_from DESC) AS rn
    FROM product_prices
)
SELECT sku, price_cents / 100.0 AS price_kes, valid_from
FROM ranked
WHERE rn = 1;

-- 2. Top-selling product per shop per week (by event time, sold_at).
WITH weekly AS (
    SELECT t.shop_id, date_trunc('week', s.sold_at) AS week, i.sku, SUM(i.qty) AS units
    FROM sales s
    JOIN tills t      USING (till_id)
    JOIN sale_items i USING (receipt_id)
    GROUP BY t.shop_id, date_trunc('week', s.sold_at), i.sku
)
SELECT *
FROM (
    SELECT weekly.*, RANK() OVER (PARTITION BY shop_id, week ORDER BY units DESC) AS rnk
    FROM weekly
) ranked
WHERE rnk = 1;

-- 3. TODO: the same with DENSE_RANK; create a tie and explain the difference in one sentence.
-- 4. TODO: 7-day rolling revenue per shop: SUM(revenue) OVER (PARTITION BY shop_id ORDER BY day
--          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW).
