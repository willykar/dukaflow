package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/willykar/dukaflow/go/internal/auth"
	"github.com/willykar/dukaflow/go/internal/metrics"
	"github.com/willykar/dukaflow/go/internal/sale"
)

type fakePub struct {
	sales, dlq int
	fail       bool
}

func (f *fakePub) PublishSale(context.Context, sale.Sale) error {
	if f.fail {
		return errors.New("kafka down")
	}
	f.sales++
	return nil
}
func (f *fakePub) PublishDLQ(context.Context, []byte, string) error { f.dlq++; return nil }

type failingStore struct{}

func (failingStore) ShopForKeyHash(context.Context, string) (int, bool, error) {
	return 0, false, errors.New("db down")
}

const shop1Key = "dk_test_shop1"

func newHandler(p *fakePub, store auth.Store) (http.Handler, *metrics.Registry) {
	now := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	reg := metrics.New()
	h := Handler{
		Pub:     p,
		Auth:    &auth.Cached{Store: store, TTL: time.Minute},
		Now:     func() time.Time { return now },
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: reg,
	}
	return h.Routes(), reg
}

func keys() auth.Store { return auth.Static{auth.HashKey(shop1Key): 1} }

const valid = `{"receipt_id":"r-1","shop_id":1,"till_id":11,"sold_at":"2026-01-10T11:00:00Z",
"synced_at":"2026-01-10T11:00:05Z","payment_method":"cash","items":[{"sku":"UNGA-2KG","qty":1,"unit_price_cents":19500}]}`

func post(h http.Handler, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/sales", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestValidSaleIsQueued(t *testing.T) {
	p := &fakePub{}
	h, reg := newHandler(p, keys())
	rec := post(h, shop1Key, valid)
	if rec.Code != http.StatusAccepted || p.sales != 1 || p.dlq != 0 {
		t.Fatalf("status=%d sales=%d dlq=%d", rec.Code, p.sales, p.dlq)
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("every response carries a request id for tracing")
	}
	if reg.Counter("dukaflow_gateway_sales_accepted_total", "").Value() != 1 {
		t.Fatal("accepted counter not incremented")
	}
}

func TestMissingOrUnknownKeyIsUnauthorized(t *testing.T) {
	p := &fakePub{}
	h, _ := newHandler(p, keys())
	for _, key := range []string{"", "dk_wrong"} {
		if rec := post(h, key, valid); rec.Code != http.StatusUnauthorized {
			t.Fatalf("key %q: status %d, want 401", key, rec.Code)
		}
	}
	if p.sales+p.dlq != 0 {
		t.Fatal("unauthenticated requests must not reach Kafka at all")
	}
}

func TestKeyCannotPostAnotherShopsSale(t *testing.T) {
	p := &fakePub{}
	h, _ := newHandler(p, keys())
	other := strings.Replace(valid, `"shop_id":1`, `"shop_id":2`, 1)
	if rec := post(h, shop1Key, other); rec.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403", rec.Code)
	}
	if p.sales != 0 {
		t.Fatal("cross-shop sale was queued")
	}
}

func TestInvalidSaleGoesToDLQ(t *testing.T) {
	p := &fakePub{}
	h, _ := newHandler(p, keys())
	bad := strings.Replace(valid, `"qty":1`, `"qty":-1`, 1)
	if rec := post(h, shop1Key, bad); rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", rec.Code)
	}
	if p.dlq != 1 || p.sales != 0 {
		t.Fatalf("sales=%d dlq=%d: a bad sale must reach the DLQ, not the sales topic", p.sales, p.dlq)
	}
}

func TestNotJSONIs400(t *testing.T) {
	h, _ := newHandler(&fakePub{}, keys())
	if rec := post(h, shop1Key, "not json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestKafkaDownIs503SoTheTillRetries(t *testing.T) {
	h, _ := newHandler(&fakePub{fail: true}, keys())
	if rec := post(h, shop1Key, valid); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", rec.Code)
	}
}

func TestKeyStoreDownIs503(t *testing.T) {
	h, _ := newHandler(&fakePub{}, failingStore{})
	if rec := post(h, shop1Key, valid); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503 (retryable), not 401", rec.Code)
	}
}

func TestHealthReadyAndMetrics(t *testing.T) {
	h, _ := newHandler(&fakePub{}, keys())
	post(h, shop1Key, valid)
	for path, want := range map[string]int{"/healthz": 200, "/readyz": 200, "/metrics": 200} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != want {
			t.Fatalf("%s: %d", path, rec.Code)
		}
		if path == "/metrics" && !strings.Contains(rec.Body.String(), "dukaflow_gateway_sales_accepted_total 1") {
			t.Fatalf("metrics output missing counter:\n%s", rec.Body.String())
		}
	}
}
