from dataclasses import replace
from datetime import datetime

from generator.simulate import Config, is_valid, sale_total_cents, simulate

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
