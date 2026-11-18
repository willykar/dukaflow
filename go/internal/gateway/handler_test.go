package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/willykar/dukaflow/go/internal/sale"
)

type fakePub struct{ sales, dlq int }

func (f *fakePub) PublishSale(context.Context, sale.Sale) error     { f.sales++; return nil }
func (f *fakePub) PublishDLQ(context.Context, []byte, string) error { f.dlq++; return nil }

func newHandler(p *fakePub) http.Handler {
	now := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	return Handler{Pub: p, Now: func() time.Time { return now }}.Routes()
}

const valid = `{"receipt_id":"r-1","shop_id":1,"till_id":11,"sold_at":"2026-01-10T11:00:00Z",
"synced_at":"2026-01-10T11:00:05Z","payment_method":"cash","items":[{"sku":"UNGA-2KG","qty":1,"unit_price_cents":19500}]}`

func post(h http.Handler, body string) int {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/sales", strings.NewReader(body)))
	return rec.Code
}

func TestValidSaleIsQueued(t *testing.T) {
	p := &fakePub{}
	if code := post(newHandler(p), valid); code != http.StatusAccepted {
		t.Fatalf("status %d, want 202", code)
	}
	if p.sales != 1 || p.dlq != 0 {
		t.Fatalf("sales=%d dlq=%d", p.sales, p.dlq)
	}
}

func TestInvalidSaleGoesToDLQ(t *testing.T) {
	p := &fakePub{}
	bad := strings.Replace(valid, `"qty":1`, `"qty":-1`, 1)
	if code := post(newHandler(p), bad); code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", code)
	}
	if p.dlq != 1 || p.sales != 0 {
		t.Fatalf("sales=%d dlq=%d: a bad sale must reach the DLQ, not the sales topic", p.sales, p.dlq)
	}
}

func TestHealthz(t *testing.T) {
	rec := httptest.NewRecorder()
	newHandler(&fakePub{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz %d", rec.Code)
	}
}
