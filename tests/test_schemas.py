import pytest
from pydantic import ValidationError

from agent.schemas import AskPlan, MpesaTransaction, QueryName


def test_valid_mpesa_row():
    t = MpesaTransaction(mpesa_ref="QK7H2LM9XA", amount_cents=19500, paid_at="2026-01-10T11:00:00Z")
    assert t.amount_cents == 19500


@pytest.mark.parametrize("ref", ["qk7h2lm9xa", "SHORT", "QK7H2LM9XA1"])
def test_bad_mpesa_ref_rejected(ref):
    with pytest.raises(ValidationError):
        MpesaTransaction(mpesa_ref=ref, amount_cents=100, paid_at="2026-01-10T11:00:00Z")


def test_raw_phone_number_rejected():
    with pytest.raises(ValidationError):
        MpesaTransaction(mpesa_ref="QK7H2LM9XA", amount_cents=100,
                         paid_at="2026-01-10T11:00:00Z", payer_phone_hash="0712345678")


def test_ask_plan_only_accepts_catalogue_queries():
    assert AskPlan(query="top_products", shop_id=1).query is QueryName.top_products
    with pytest.raises(ValidationError):
        AskPlan(query="DROP TABLE sales", shop_id=1)
