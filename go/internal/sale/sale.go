// Package sale defines a till sale and the rules every layer enforces on it.
package sale

import (
	"errors"
	"fmt"
	"time"
)

// ClockSkew is how far a till's clock may run ahead of ours before a sale counts as "in the future".
const ClockSkew = 5 * time.Minute

type Item struct {
	SKU            string `json:"sku"`
	Qty            int    `json:"qty"`
	UnitPriceCents int    `json:"unit_price_cents"`
}

// Sale is one receipt. ReceiptID is generated on the till and is the idempotency key end to end.
type Sale struct {
	ReceiptID     string    `json:"receipt_id"`
	ShopID        int       `json:"shop_id"`
	TillID        int       `json:"till_id"`
	SoldAt        time.Time `json:"sold_at"`   // event time
	SyncedAt      time.Time `json:"synced_at"` // processing time; days later for an offline till
	PaymentMethod string    `json:"payment_method"`
	CustomerID    *int      `json:"customer_id"` // nil for a walk-in customer
	CustomerName  *string   `json:"customer_name"`
	Items         []Item    `json:"items"`
}

// Validate returns an error for any sale that must go to the dead-letter topic instead of the database.
func (s Sale) Validate(now time.Time) error {
	switch {
	case s.ReceiptID == "":
		return errors.New("missing receipt_id")
	case s.ShopID <= 0 || s.TillID <= 0:
		return errors.New("shop_id and till_id must be positive")
	case s.PaymentMethod != "cash" && s.PaymentMethod != "mpesa":
		return fmt.Errorf("unknown payment_method %q", s.PaymentMethod)
	case len(s.Items) == 0:
		return errors.New("sale has no items")
	case s.SoldAt.IsZero():
		return errors.New("missing sold_at")
	case s.SoldAt.After(now.Add(ClockSkew)):
		return errors.New("sold_at is in the future")
	case !s.SyncedAt.IsZero() && s.SyncedAt.Before(s.SoldAt.Add(-ClockSkew)):
		return errors.New("synced_at is before sold_at")
	}
	for i, it := range s.Items {
		if it.SKU == "" {
			return fmt.Errorf("item %d: missing sku", i)
		}
		if it.Qty <= 0 {
			return fmt.Errorf("item %d: qty must be positive", i)
		}
		if it.UnitPriceCents < 0 {
			return fmt.Errorf("item %d: negative price", i)
		}
	}
	return nil
}

// TotalCents is the receipt total, used for M-Pesa reconciliation.
func (s Sale) TotalCents() int {
	total := 0
	for _, it := range s.Items {
		total += it.Qty * it.UnitPriceCents
	}
	return total
}

// Lateness is how long the sale took to reach us: the late-arriving-data measure.
func (s Sale) Lateness() time.Duration {
	if s.SyncedAt.IsZero() {
		return 0
	}
	return s.SyncedAt.Sub(s.SoldAt)
}
