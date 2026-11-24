SET search_path TO duka;

-- Walk-in customers have no customer row: show a label instead of NULL.
SELECT s.receipt_id, COALESCE(c.display_name, 'Walk-in') AS customer
FROM sales s
LEFT JOIN customers c USING (customer_id)
LIMIT 20;

-- COALESCE is right for a display label (above) and for "no sales = 0 revenue". It is WRONG for data that is
-- unknown rather than zero: COALESCE(customer_id, 0) would invent a customer 0, and averaging a missing
-- measurement as 0 drags the average down. Unknown should stay NULL.

-- Average basket per till without dividing by zero for a till with no sales yet.
SELECT t.till_id,
       COUNT(DISTINCT s.receipt_id)                                       AS transactions,
       SUM(i.qty * i.unit_price_cents) / 100.0                            AS revenue_kes,
       SUM(i.qty * i.unit_price_cents) / 100.0 / NULLIF(COUNT(DISTINCT s.receipt_id), 0) AS avg_basket_kes
FROM tills t
LEFT JOIN sales s      USING (till_id)
LEFT JOIN sale_items i USING (receipt_id)
GROUP BY t.till_id;
