SET search_path TO duka;

-- WHERE filters rows BEFORE grouping; HAVING filters groups AFTER aggregating.
-- Beverages (WHERE: a row filter) that sold more than 100 units this month (HAVING: a group filter).
SELECT p.sku, p.name, SUM(i.qty) AS units
FROM sale_items i
JOIN sales s      USING (receipt_id)
JOIN products p   USING (sku)
JOIN categories c USING (category_id)
WHERE c.name = 'Beverages'
  AND s.sold_at >= date_trunc('month', now())
GROUP BY p.sku, p.name
HAVING SUM(i.qty) > 100
ORDER BY units DESC;

-- TODO: move SUM(i.qty) > 100 into WHERE, read the error, and write down why it fails.
