"""Pydantic models every Gemini output must satisfy before it is used (phase 5)."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class MpesaTransaction(BaseModel):
    """One row parsed from an M-Pesa statement."""
    mpesa_ref: str = Field(pattern=r"^[A-Z0-9]{10}$")
    amount_cents: int = Field(gt=0)
    paid_at: datetime
    payer_phone_hash: Optional[str] = Field(default=None, pattern=r"^[0-9a-f]{64}$")  # sha256, never raw


class InvoiceLine(BaseModel):
    """One line parsed from a supplier invoice."""
    supplier: str = Field(min_length=2)
    item: str = Field(min_length=2)
    qty: int = Field(gt=0)
    unit_price_cents: int = Field(ge=0)


class QueryName(str, Enum):
    top_products = "top_products"
    sales_by_day = "sales_by_day"
    stock_cover = "stock_cover"
    unmatched_payments = "unmatched_payments"


class AskPlan(BaseModel):
    """What Gemini returns for "ask your shop": a CHOICE from the catalogue, never SQL."""
    query: QueryName
    shop_id: int = Field(gt=0)
    days: int = Field(default=7, ge=1, le=365)
    limit: int = Field(default=5, ge=1, le=50)
