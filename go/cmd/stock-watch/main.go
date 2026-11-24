// stock-watch reads pos.sales in its OWN consumer group ("stock-watch"), so it receives every sale
// independently of the sales-consumer. After each sale it checks the products sold and raises a low-stock
// alert (once) on pos.alerts; a periodic sweep clears recovered alerts and catches any it missed.
package main

import (
	"os"
	"time"

	"github.com/willykar/dukaflow/go/internal/bus"
	"github.com/willykar/dukaflow/go/internal/config"
	"github.com/willykar/dukaflow/go/internal/consume"
	"github.com/willykar/dukaflow/go/internal/metrics"
	"github.com/willykar/dukaflow/go/internal/pipeline"
	"github.com/willykar/dukaflow/go/internal/runtime"
	"github.com/willykar/dukaflow/go/internal/store"
)

const group = "stock-watch"

func main() {
	cfg := config.FromEnv()
	log := runtime.Logger("stock-watch")
	ctx, stop := runtime.SignalContext()
	defer stop()

	topics := bus.Topics{Sales: cfg.TopicSales, DLQ: cfg.TopicDLQ, Alerts: cfg.TopicAlerts}
	if err := bus.EnsureTopics(ctx, cfg.Brokers, topics.Specs()); err != nil {
		log.Error("kafka not reachable", "err", err)
		os.Exit(1)
	}
	db, err := store.Connect(ctx, cfg.PostgresURL)
	if err != nil {
		log.Error("postgres not reachable", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	prod := bus.NewProducer(cfg.Brokers, topics)
	defer prod.Close()

	reg := metrics.New()
	if cfg.MetricsAddr != "" {
		go runtime.Serve(ctx, runtime.OpsServer(cfg.MetricsAddr, reg), log)
	}
	w := pipeline.StockWatcher{Store: db, Pub: prod, Log: log, Metrics: reg, Now: time.Now}

	go func() {
		t := time.NewTicker(cfg.SweepEvery)
		defer t.Stop()
		for {
			if err := w.Sweep(ctx); err != nil && ctx.Err() == nil {
				log.Error("sweep failed", "err", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()

	r := runtime.Reader(cfg.Brokers, cfg.TopicSales, group)
	defer r.Close()
	log.Info("watching stock", "topic", cfg.TopicSales, "group", group, "sweep_every", cfg.SweepEvery.String())
	loop := consume.Loop{Fetch: r, DLQ: prod, Handle: w.Handler(), Log: log}
	if err := loop.Run(ctx); err != nil {
		log.Error("stopped", "err", err)
		os.Exit(1)
	}
	log.Info("stopped cleanly")
}
