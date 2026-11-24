package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"

	"github.com/willykar/dukaflow/go/internal/bus"
	"github.com/willykar/dukaflow/go/internal/consume"
	"github.com/willykar/dukaflow/go/internal/metrics"
	"github.com/willykar/dukaflow/go/internal/sale"
	"github.com/willykar/dukaflow/go/internal/store"
)

var now = time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)

const saleJSON = `{"receipt_id":"5f0c6c1e-8a51-4a7e-9a55-0b1e0c2a9f11","shop_id":1,"till_id":11,
"sold_at":"2026-01-10T11:00:00Z","synced_at":"2026-01-10T11:00:05Z","payment_method":"cash",
"items":[{"sku":"UNGA-2KG","qty":2,"unit_price_cents":19500},{"sku":"MILK-500","qty":1,"unit_price_cents":6500}]}`

type fakeSaver struct {
	inserted bool
	err      error
}

func (f fakeSaver) SaveSale(context.Context, sale.Sale) (bool, error) { return f.inserted, f.err }

func handle(db SaleSaver, value string) (error, *metrics.Registry) {
	reg := metrics.New()
	h := SalesHandler(db, reg, func() time.Time { return now })
	return h(context.Background(), kafka.Message{Value: []byte(value)}), reg
}

func TestSalesHandlerCountsInsertsAndDuplicates(t *testing.T) {
	err, reg := handle(fakeSaver{inserted: true}, saleJSON)
	if err != nil || reg.Counter("dukaflow_consumer_sales_inserted_total", "").Value() != 1 {
		t.Fatalf("insert: err=%v", err)
	}
	err, reg = handle(fakeSaver{inserted: false}, saleJSON)
	if err != nil || reg.Counter("dukaflow_consumer_duplicates_total", "").Value() != 1 {
		t.Fatalf("duplicate must succeed quietly: err=%v", err)
	}
}

func TestSalesHandlerClassifiesFailures(t *testing.T) {
	cases := map[string]struct {
		db        SaleSaver
		value     string
		permanent bool
	}{
		"not json":           {fakeSaver{}, "{", true},
		"invalid sale":       {fakeSaver{}, `{"receipt_id":"x","shop_id":1,"till_id":11,"payment_method":"cash","items":[]}`, true},
		"unknown sku (FK)":   {fakeSaver{err: &pgconn.PgError{Code: "23503"}}, saleJSON, true},
		"till of other shop": {fakeSaver{err: store.ErrTillNotInShop}, saleJSON, true},
		"database down":      {fakeSaver{err: errors.New("connection refused")}, saleJSON, false},
	}
	for name, c := range cases {
		err, _ := handle(c.db, c.value)
		if err == nil || consume.IsPermanent(err) != c.permanent {
			t.Errorf("%s: err=%v permanent=%v, want permanent=%v", name, err, consume.IsPermanent(err), c.permanent)
		}
	}
}

type fakeStock struct {
	levels  []store.Level
	open    map[string]bool
	cleared int64
}

func (f *fakeStock) Levels(context.Context, int, []string) ([]store.Level, error) {
	return f.levels, nil
}
func (f *fakeStock) LowStock(context.Context) ([]store.Level, error) { return f.levels, nil }
func (f *fakeStock) ClearRecovered(context.Context) (int64, error)   { return f.cleared, nil }
func (f *fakeStock) OpenAlert(_ context.Context, l store.Level) (bool, error) {
	if f.open[l.SKU] {
		return false, nil
	}
	f.open[l.SKU] = true
	return true, nil
}

type fakeAlerts struct{ sent []bus.Alert }

func (f *fakeAlerts) PublishAlert(_ context.Context, a bus.Alert) error {
	f.sent = append(f.sent, a)
	return nil
}

func TestStockWatcherAlertsOnceUntilRecovered(t *testing.T) {
	st := &fakeStock{open: map[string]bool{}, levels: []store.Level{
		{ShopID: 1, SKU: "UNGA-2KG", OnHand: 12, ReorderLevel: 40},  // low
		{ShopID: 1, SKU: "MILK-500", OnHand: 120, ReorderLevel: 40}, // fine
	}}
	pub := &fakeAlerts{}
	w := StockWatcher{Store: st, Pub: pub, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: metrics.New(), Now: func() time.Time { return now }}
	h := w.Handler()
	for i := 0; i < 3; i++ { // three sales of the same low product
		if err := h(context.Background(), kafka.Message{Value: []byte(saleJSON)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pub.sent) != 1 || pub.sent[0].SKU != "UNGA-2KG" || pub.sent[0].Type != "low_stock" {
		t.Fatalf("alerts %+v: want exactly one low_stock alert for UNGA-2KG", pub.sent)
	}
}
