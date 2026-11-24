from dataclasses import replace
from datetime import datetime

from collections import Counter

from generator.simulate import (CUSTOMERS, Config, categories, is_month_end, is_valid, sale_total_cents,
                                shops, simulate, till_ids)

CFG = Config(seed=7, shops=3, days=14)


def _ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def test_same_seed_same_data():
    assert simulate(CFG) == simulate(CFG)


def test_different_seed_different_data():
    assert simulate(CFG) != simulate(replace(CFG, seed=8))


def test_clean_config_produces_only_valid_unique_sales():
    data = simulate(replace(CFG, late_rate=0, retry_rate=0, bad_rate=0))
    ids = [s["receipt_id"] for s in data["sales"]]
    assert len(ids) == len(set(ids))
    assert all(is_valid(s) for s in data["sales"])


def test_problems_are_injected():
    data = simulate(replace(CFG, late_rate=0.2, retry_rate=0.1, bad_rate=0.05))
    sales = data["sales"]
    ids = [s["receipt_id"] for s in sales]
    late = [s for s in sales if (_ts(s["synced_at"]) - _ts(s["sold_at"])).total_seconds() > 3600]
    assert len(ids) > len(set(ids)), "retries should duplicate receipt ids"
    assert late, "offline tills should produce late syncs"
    assert any(not is_valid(s) for s in sales), "bad records should be present"


def test_late_sync_never_more_than_three_days():
    data = simulate(replace(CFG, late_rate=1.0))
    assert all((_ts(s["synced_at"]) - _ts(s["sold_at"])).total_seconds() <= 72 * 3600 for s in data["sales"])


def test_matched_mpesa_amounts_equal_sale_totals():
    data = simulate(replace(CFG, bad_rate=0, retry_rate=0))
    by_receipt = {s["receipt_id"]: s for s in data["sales"]}
    matched = [m for m in data["mpesa"] if m["receipt_id"]]
    assert matched
    assert all(m["amount_cents"] == sale_total_cents(by_receipt[m["receipt_id"]]) for m in matched)


def test_month_end_is_busier_than_mid_month():
    data = simulate(replace(CFG, shops=5, days=120, late_rate=0, retry_rate=0, bad_rate=0))
    per_day = Counter(s["sold_at"][:10] for s in data["sales"])
    end = [n for d, n in per_day.items() if is_month_end(datetime.fromisoformat(d))]
    mid = [n for d, n in per_day.items() if not is_month_end(datetime.fromisoformat(d))]
    assert sum(end) / len(end) > sum(mid) / len(mid)


def test_month_end_days():
    assert is_month_end(datetime(2026, 1, 30)) and is_month_end(datetime(2026, 2, 1))
    assert not is_month_end(datetime(2026, 1, 15))


def test_reference_data_is_consistent():
    data = simulate(CFG)
    shop_ids = {s[0] for s in shops(CFG.shops)}
    valid_tills = {t for sid in shop_ids for t in till_ids(sid)}
    customer_ids = {c[0] for c in CUSTOMERS}
    assert {s["shop_id"] for s in data["sales"]} == shop_ids
    assert {s["till_id"] for s in data["sales"]} <= valid_tills
    assert {s["customer_id"] for s in data["sales"]} - {None} <= customer_ids
    assert len(categories()) == len({c[1] for c in categories()})


def test_shop_names_unique_beyond_name_list():
    names = [s[1] for s in shops(20)]
    assert len(names) == len(set(names))


def test_stock_never_goes_negative():
    from generator.simulate import PAR_LEVEL
    data = simulate(replace(CFG, shops=2, days=21, bad_rate=0, retry_rate=0))
    stock = Counter()
    events = [(d["delivered_at"], 0, d["shop_id"], d["sku"], d["qty"]) for d in data["deliveries"]]
    events += [(s["sold_at"], 1, s["shop_id"], i["sku"], -i["qty"]) for s in data["sales"] for i in s["items"]]
    for _ts, _order, shop, sku, qty in sorted(events):
        stock[(shop, sku)] += qty
        assert 0 <= stock[(shop, sku)] <= PAR_LEVEL


def test_deliveries_restock_on_mondays():
    data = simulate(replace(CFG, shops=1, days=15))
    days = {datetime.fromisoformat(d["delivered_at"].replace("Z", "+00:00")).date() for d in data["deliveries"]}
    assert all(d.weekday() == 0 or d == CFG.start.date() for d in days)
    assert len(days) >= 2


def test_live_sale_is_valid_or_deliberately_bad():
    import random
    from datetime import timezone
    from generator.simulate import make_live_sale
    rng, now = random.Random(1), datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    clean = replace(CFG, bad_rate=0, late_rate=0)
    sale = make_live_sale(rng, clean, 1, now)
    assert is_valid(sale, now=now) and sale["synced_at"] == "2026-03-01T12:00:00Z"
