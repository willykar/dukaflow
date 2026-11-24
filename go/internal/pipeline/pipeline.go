// Package pipeline holds the per-message logic of the two consumers: saving sales and watching stock.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/willykar/dukaflow/go/internal/bus"
	"github.com/willykar/dukaflow/go/internal/consume"
	"github.com/willykar/dukaflow/go/internal/metrics"
	"github.com/willykar/dukaflow/go/internal/sale"
	"github.com/willykar/dukaflow/go/internal/store"
)

func decode(m kafka.Message, now time.Time) (sale.Sale, error) {
	var s sale.Sale
	if err := json.Unmarshal(m.Value, &s); err != nil {
		return s, consume.Permanent(fmt.Errorf("decode: %w", err))
	}
	if err := s.Validate(now); err != nil { // the gateway checked too; never trust the topic blindly
		return s, consume.Permanent(err)
	}
	return s, nil
}

// SaleSaver is the part of store.PG the sales consumer needs.
type SaleSaver interface {
	SaveSale(ctx context.Context, s sale.Sale) (bool, error)
}

// SalesHandler saves each sale; duplicates are counted and skipped, permanent DB errors dead-lettered.
func SalesHandler(db SaleSaver, reg *metrics.Registry, now func() time.Time) consume.Handler {
	inserted := reg.Counter("dukaflow_consumer_sales_inserted_total", "Sales written to PostgreSQL")
	dupes := reg.Counter("dukaflow_consumer_duplicates_total", "Sales already stored (retries, redeliveries)")
	return func(ctx context.Context, m kafka.Message) error {
		s, err := decode(m, now())
		if err != nil {
			return err
		}
		ok, err := db.SaveSale(ctx, s)
		if err != nil {
			if store.IsPermanent(err) {
				return consume.Permanent(err)
			}
			return err
		}
		if ok {
			inserted.Inc()
		} else {
			dupes.Inc()
		}
		return nil
	}
}

// StockStore is the part of store.PG the stock watcher needs.
type StockStore interface {
	Levels(ctx context.Context, shopID int, skus []string) ([]store.Level, error)
	LowStock(ctx context.Context) ([]store.Level, error)
	OpenAlert(ctx context.Context, l store.Level) (bool, error)
	ClearRecovered(ctx context.Context) (int64, error)
}

type AlertPublisher interface {
	PublishAlert(ctx context.Context, a bus.Alert) error
}

// StockWatcher raises a low-stock alert the first time a product falls to its reorder level, and closes
// it when stock recovers. PostgreSQL is the source of truth (the open alert row); the Kafka alert is the
// push notification. Because the watcher may read stock just before the sales consumer writes a sale,
// a periodic Sweep re-checks everything, so a missed crossing is caught within one sweep interval.
type StockWatcher struct {
	Store   StockStore
	Pub     AlertPublisher
	Log     *slog.Logger
	Metrics *metrics.Registry
	Now     func() time.Time
}

func (w StockWatcher) Handler() consume.Handler {
	return func(ctx context.Context, m kafka.Message) error {
		s, err := decode(m, w.Now())
		if err != nil {
			return err
		}
		skus := make([]string, 0, len(s.Items))
		for _, it := range s.Items {
			skus = append(skus, it.SKU)
		}
		levels, err := w.Store.Levels(ctx, s.ShopID, skus)
		if err != nil {
			return err
		}
		return w.alertLow(ctx, levels)
	}
}

// Sweep clears recovered alerts and raises any missed ones.
func (w StockWatcher) Sweep(ctx context.Context) error {
	cleared, err := w.Store.ClearRecovered(ctx)
	if err != nil {
		return err
	}
	if cleared > 0 {
		w.Metrics.Counter("dukaflow_stock_alerts_cleared_total", "Alerts closed after a restock").Add(cleared)
	}
	low, err := w.Store.LowStock(ctx)
	if err != nil {
		return err
	}
	return w.alertLow(ctx, low)
}

func (w StockWatcher) alertLow(ctx context.Context, levels []store.Level) error {
	for _, l := range levels {
		if l.OnHand > l.ReorderLevel {
			continue
		}
		opened, err := w.Store.OpenAlert(ctx, l)
		if err != nil {
			return err
		}
		if !opened {
			continue // already alerted; stay quiet until it recovers
		}
		w.Metrics.Counter("dukaflow_stock_alerts_opened_total", "Low-stock alerts raised").Inc()
		w.Log.Info("low stock", "shop_id", l.ShopID, "sku", l.SKU, "on_hand", l.OnHand, "reorder_level", l.ReorderLevel)
		if err := w.Pub.PublishAlert(ctx, bus.Alert{Type: "low_stock", ShopID: l.ShopID, SKU: l.SKU,
			OnHand: l.OnHand, ReorderLevel: l.ReorderLevel, At: w.Now().UTC()}); err != nil {
			// The open alert row is already saved and the dashboard reads it; only the push failed.
			w.Metrics.Counter("dukaflow_stock_alert_publish_errors_total", "Alerts saved but not pushed").Inc()
			w.Log.Error("alert publish failed", "err", err)
		}
	}
	return nil
}
