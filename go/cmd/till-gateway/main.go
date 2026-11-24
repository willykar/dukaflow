// till-gateway receives sales from tills over HTTPS, authenticates each till by API key, and queues
// valid sales on Kafka (invalid ones go to the dead-letter topic). See internal/gateway for the contract.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/willykar/dukaflow/go/internal/auth"
	"github.com/willykar/dukaflow/go/internal/bus"
	"github.com/willykar/dukaflow/go/internal/config"
	"github.com/willykar/dukaflow/go/internal/gateway"
	"github.com/willykar/dukaflow/go/internal/metrics"
	"github.com/willykar/dukaflow/go/internal/runtime"
	"github.com/willykar/dukaflow/go/internal/store"
)

func main() {
	cfg := config.FromEnv()
	log := runtime.Logger("till-gateway")
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

	h := gateway.Handler{
		Pub:     prod,
		Auth:    &auth.Cached{Store: db, TTL: cfg.AuthCache},
		Now:     time.Now,
		Log:     log,
		Metrics: metrics.New(),
		Ready: func(ctx context.Context) error {
			return errors.Join(db.Ping(ctx), bus.Ping(ctx, cfg.Brokers))
		},
	}
	srv := &http.Server{
		Addr:              cfg.GatewayAddr,
		Handler:           h.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	if err := runtime.Serve(ctx, srv, log); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
	log.Info("stopped cleanly")
}
