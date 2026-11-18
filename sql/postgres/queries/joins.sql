SET search_path TO duka;

-- M-Pesa reconciliation. FULL OUTER JOIN keeps unmatched rows from BOTH sides:
--   a payment with no sale, and a sale marked M-Pesa with no payment.
-- Matching rule here: same shop, same amount, paid within 10 minutes of the sale.
WITH mpesa_sales AS (
    SELECT s.receipt_id, t.shop_id, s.sold_at, SUM(i.qty * i.unit_price_cents) AS total_cents
    FROM sales s
    JOIN tills t      USING (till_id)
    JOIN sale_items i USING (receipt_id)
    WHERE s.payment_method = 'mpesa'
    GROUP BY s.receipt_id, t.shop_id, s.sold_at
)
SELECT ms.receipt_id, m.mpesa_ref,
       CASE WHEN m.mpesa_ref IS NULL THEN 'sale with no payment'
            WHEN ms.receipt_id IS NULL THEN 'payment with no sale'
       END AS problem
FROM mpesa_sales ms
FULL OUTER JOIN mpesa_transactions m
  ON  m.shop_id = ms.shop_id
  AND m.amount_cents = ms.total_cents
  AND m.paid_at BETWEEN ms.sold_at - interval '10 minutes' AND ms.sold_at + interval '10 minutes'
WHERE m.mpesa_ref IS NULL OR ms.receipt_id IS NULL;

-- TODO: write the INNER and LEFT JOIN versions and note what each one silently hides.
