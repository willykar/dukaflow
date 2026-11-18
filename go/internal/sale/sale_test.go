package sale

import (
	"testing"
	"time"
)

var now = time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)

func good() Sale {
	return Sale{
		ReceiptID: "5f0c6c1e-8a51-4a7e-9a55-0b1e0c2a9f11", ShopID: 1, TillID: 11,
		SoldAt: now.Add(-time.Hour), SyncedAt: now.Add(-time.Hour + 10*time.Second),
		PaymentMethod: "mpesa",
		Items:         []Item{{SKU: "UNGA-2KG", Qty: 2, UnitPriceCents: 19500}, {SKU: "MILK-500", Qty: 1, UnitPriceCents: 6500}},
	}
}

func TestValidSale(t *testing.T) {
	if err := good().Validate(now); err != nil {
		t.Fatalf("valid sale rejected: %v", err)
	}
}

func TestInvalidSales(t *testing.T) {
	cases := map[string]func(*Sale){
		"missing receipt": func(s *Sale) { s.ReceiptID = "" },
		"negative qty":    func(s *Sale) { s.Items[0].Qty = -1 },
		"future sale":     func(s *Sale) { s.SoldAt = now.Add(time.Hour) },
		"bad payment":     func(s *Sale) { s.PaymentMethod = "cheque" },
		"no items":        func(s *Sale) { s.Items = nil },
		"synced before":   func(s *Sale) { s.SyncedAt = s.SoldAt.Add(-time.Hour) },
	}
	for name, mutate := range cases {
		s := good()
		s.Items = append([]Item(nil), s.Items...)
		mutate(&s)
		if s.Validate(now) == nil {
			t.Errorf("%s: expected a validation error (this sale belongs in the DLQ)", name)
		}
	}
}

func TestOfflineTillIsLateButValid(t *testing.T) {
	s := good()
	s.SyncedAt = s.SoldAt.Add(72 * time.Hour)
	if err := s.Validate(now.Add(72 * time.Hour)); err != nil {
		t.Fatalf("a late sale is valid, just late: %v", err)
	}
	if s.Lateness() != 72*time.Hour {
		t.Fatalf("lateness = %v, want 72h", s.Lateness())
	}
}

func TestTotalCents(t *testing.T) {
	if got := good().TotalCents(); got != 2*19500+6500 {
		t.Fatalf("total = %d", got)
	}
}
