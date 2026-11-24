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

-- 3. RANK vs DENSE_RANK on a tie. With units 50, 50, 40:
--      RANK       -> 1, 1, 3   (skips 2: "two products share first place, the next is third")
--      DENSE_RANK -> 1, 1, 2   (no gap: "the second-highest sales figure")
--    Use RANK for "top N places", DENSE_RANK for "top N distinct values", ROW_NUMBER for "exactly one row".
WITH weekly AS (
    SELECT t.shop_id, date_trunc('week', s.sold_at) AS week, i.sku, SUM(i.qty) AS units
    FROM sales s
    JOIN tills t      USING (till_id)
    JOIN sale_items i USING (receipt_id)
    GROUP BY 1, 2, 3
)
SELECT shop_id, week, sku, units,
       RANK()       OVER w AS rnk,
       DENSE_RANK() OVER w AS dense_rnk,
       ROW_NUMBER() OVER w AS row_num
FROM weekly
WINDOW w AS (PARTITION BY shop_id, week ORDER BY units DESC)
ORDER BY shop_id, week, units DESC
LIMIT 30;

-- 4. 7-day rolling revenue per shop. Build one row per shop per day FIRST (generate_series), so a day with
--    no sales counts as 0; otherwise "6 PRECEDING rows" would silently reach back more than 6 days.
WITH days AS (
    SELECT sh.shop_id, d::date AS day
    FROM shops sh
    CROSS JOIN generate_series((SELECT min(sold_at)::date FROM sales),
                               (SELECT max(sold_at)::date FROM sales), interval '1 day') AS d
),
daily AS (
    SELECT t.shop_id, s.sold_at::date AS day, SUM(i.qty * i.unit_price_cents) / 100.0 AS revenue_kes
    FROM sales s
    JOIN tills t      USING (till_id)
    JOIN sale_items i USING (receipt_id)
    GROUP BY 1, 2
)
SELECT d.shop_id, d.day,
       COALESCE(daily.revenue_kes, 0) AS revenue_kes,
       SUM(COALESCE(daily.revenue_kes, 0)) OVER (
           PARTITION BY d.shop_id ORDER BY d.day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ) AS revenue_7d_kes
FROM days d
LEFT JOIN daily USING (shop_id, day)
ORDER BY d.shop_id, d.day;
