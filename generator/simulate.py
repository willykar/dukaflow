"""Synthetic duka data: sales, M-Pesa payments, offline tills, retries and bad records.

Deterministic: the same Config (including seed) always produces the same data.
"""

import random
import string
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

# sku, name, category, price in cents (KES x 100)
PRODUCTS = [
    ("UNGA-2KG", "Maize flour 2kg", "Staples", 19_500),
    ("SUGAR-1KG", "Sugar 1kg", "Staples", 16_000),
    ("RICE-1KG", "Rice 1kg", "Staples", 18_000),
    ("OIL-1L", "Cooking oil 1L", "Staples", 32_000),
    ("MILK-500", "Milk 500ml", "Dairy", 6_500),
    ("BREAD-400", "Bread 400g", "Bakery", 6_500),
    ("EGGS-1", "Egg", "Dairy", 1_500),
    ("SODA-500", "Soda 500ml", "Beverages", 8_000),
    ("WATER-1L", "Water 1L", "Beverages", 7_000),
    ("TEA-50", "Tea leaves 50g", "Beverages", 4_500),
    ("SOAP-BAR", "Bar soap", "Household", 12_000),
    ("AIRTIME-50", "Airtime 50", "Airtime", 5_000),
]

# relative sales weight per hour of day: morning and evening peaks
HOUR_WEIGHTS = {h: w for h, w in zip(range(6, 23), [2, 6, 8, 5, 4, 4, 5, 4, 3, 3, 4, 6, 8, 9, 7, 4, 2])}
VALID_PAYMENTS = ("cash", "mpesa")


@dataclass(frozen=True)
class Config:
    seed: int = 42
    shops: int = 3
    days: int = 7
    start: datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)
    sales_per_shop_day: tuple = (40, 120)
    mpesa_share: float = 0.6       # share of sales paid by M-Pesa
    late_rate: float = 0.05        # offline tills: sync hours to 3 days late
    retry_rate: float = 0.02       # same receipt sent twice
    bad_rate: float = 0.01         # invalid records for the DLQ / quarantine
    unmatched_rate: float = 0.02   # M-Pesa payments with no sale, and vice versa


def _receipt_id(rng):
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _mpesa_ref(rng):
    return "".join(rng.choices(string.ascii_uppercase + string.digits, k=10))


def _iso(ts):
    return ts.isoformat().replace("+00:00", "Z")


def _make_sale(rng, cfg, shop_id, day):
    hour = rng.choices(list(HOUR_WEIGHTS), weights=list(HOUR_WEIGHTS.values()))[0]
    sold_at = day.replace(hour=hour, minute=rng.randrange(60), second=rng.randrange(60))
    if rng.random() < cfg.late_rate:
        synced_at = sold_at + timedelta(hours=rng.uniform(2, 72))
    else:
        synced_at = sold_at + timedelta(seconds=rng.uniform(1, 30))
    lines = rng.sample(PRODUCTS, k=rng.randint(1, 4))
    return {
        "receipt_id": _receipt_id(rng),
        "shop_id": shop_id,
        "till_id": shop_id * 10 + rng.randint(1, 2),
        "sold_at": _iso(sold_at),
        "synced_at": _iso(synced_at),
        "payment_method": "mpesa" if rng.random() < cfg.mpesa_share else "cash",
        "customer_name": rng.choice([None, None, None, "Wanjiku", "Otieno", "Achieng", "Kamau"]),
        "items": [
            {"sku": sku, "qty": rng.randint(1, 3), "unit_price_cents": price}
            for sku, _name, _cat, price in lines
        ],
    }


def _corrupt(rng, sale):
    kind = rng.choice(["negative_qty", "missing_receipt", "future_sale"])
    bad = dict(sale, items=[dict(i) for i in sale["items"]])
    if kind == "negative_qty":
        bad["items"][0]["qty"] = -bad["items"][0]["qty"]
    elif kind == "missing_receipt":
        bad["receipt_id"] = ""
    else:
        bad["sold_at"] = _iso(datetime(2099, 1, 1, tzinfo=timezone.utc))
    return bad


def sale_total_cents(sale):
    return sum(i["qty"] * i["unit_price_cents"] for i in sale["items"])


def is_valid(sale, now=None):
    """The same rules the Go gateway enforces; anything failing them belongs in the DLQ."""
    now = now or datetime(2100, 1, 1, tzinfo=timezone.utc)
    if not sale.get("receipt_id") or sale.get("payment_method") not in VALID_PAYMENTS or not sale.get("items"):
        return False
    if any(i["qty"] <= 0 or i["unit_price_cents"] < 0 for i in sale["items"]):
        return False
    sold_at = datetime.fromisoformat(sale["sold_at"].replace("Z", "+00:00"))
    return sold_at.year < 2099 and sold_at <= now


def simulate(cfg=Config()):
    """Returns {"sales": [...], "mpesa": [...]}; sales include retries and bad records by design."""
    rng = random.Random(cfg.seed)
    sales, mpesa = [], []
    for d in range(cfg.days):
        day = cfg.start + timedelta(days=d)
        for shop_id in range(1, cfg.shops + 1):
            low, high = cfg.sales_per_shop_day
            n = rng.randint(low, high)
            if day.weekday() == 6:  # quieter Sundays
                n = int(n * 0.6)
            for _ in range(n):
                sale = _make_sale(rng, cfg, shop_id, day)
                if sale["payment_method"] == "mpesa" and rng.random() >= cfg.unmatched_rate:
                    mpesa.append({
                        "mpesa_ref": _mpesa_ref(rng),
                        "shop_id": shop_id,
                        "amount_cents": sale_total_cents(sale),
                        "paid_at": sale["sold_at"],
                        "receipt_id": sale["receipt_id"],  # ground truth, for testing reconciliation only
                    })
                if rng.random() < cfg.bad_rate:
                    sale = _corrupt(rng, sale)
                sales.append(sale)
                if rng.random() < cfg.retry_rate:
                    sales.append(dict(sale))  # the till retried: same receipt twice
            # a few payments that match no sale at all
            for _ in range(int(n * cfg.unmatched_rate)):
                mpesa.append({
                    "mpesa_ref": _mpesa_ref(rng),
                    "shop_id": shop_id,
                    "amount_cents": rng.randint(5, 500) * 100,
                    "paid_at": _iso(day.replace(hour=rng.randint(7, 21))),
                    "receipt_id": None,
                })
    return {"sales": sales, "mpesa": mpesa}
