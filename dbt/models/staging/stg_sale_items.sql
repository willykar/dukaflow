-- Staging: rename and cast only. Business logic belongs in intermediate and marts.
select
    receipt_id,
    line_no,
    shop_id,
    till_id,
    sku,
    qty,
    unit_price_cents,
    qty * unit_price_cents as line_total_cents,
    payment_method,
    sold_at,
    synced_at,
    datediff('minute', sold_at, synced_at) as sync_delay_minutes
from {{ source('raw', 'sale_items') }}
