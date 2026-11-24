// sales-consumer reads pos.sales in consumer group "sales-writer" and writes each sale, its lines and
// its stock movements to PostgreSQL in one transaction. Delivery: at-least-once, made exactly-once by
// the idempotent insert. Bad messages go to the dead-letter topic; a database outage pauses consumption.
package main

import (
	"os"
	"sync"
	"time"

	"github.com/willykar/dukaflow/go/internal/bus"
	"github.com/willykar/dukaflow/go/internal/config"
	"github.com/willykar/dukaflow/go/internal/consume"
	"github.com/willykar/dukaflow/go/internal/metrics"
	"github.com/willykar/dukaflow/go/internal/pipeline"
	"github.com/willykar/dukaflow/go/internal/runtime"
	"github.com/willykar/dukaflow/go/internal/store"
)

const group = "sales-writer"

func main() {
	cfg := config.FromEnv()
	log := runtime.Logger("sales-consumer")
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
	handler := pipeline.SalesHandler(db, reg, time.Now)
	results := map[string]*metrics.Counter{
		"ok":            reg.Counter("dukaflow_consumer_messages_ok_total", "Messages processed"),
		"dead_lettered": reg.Counter("dukaflow_consumer_dead_lettered_total", "Messages sent to the DLQ"),
		"retry":         reg.Counter("dukaflow_consumer_retries_total", "Transient failures retried"),
	}

	// Several readers in the same group: Kafka gives each a share of the partitions. Within a partition
	// (= a set of shops) messages are handled one at a time, in order.
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			r := runtime.Reader(cfg.Brokers, cfg.TopicSales, group)
			defer r.Close()
			loop := consume.Loop{Fetch: r, DLQ: prod, Handle: handler, Log: log.With("worker", worker),
				OnResult: func(res string) { results[res].Inc() }}
			if err := loop.Run(ctx); err != nil {
				log.Error("worker stopped", "worker", worker, "err", err)
			}
		}(i)
	}
	log.Info("consuming", "topic", cfg.TopicSales, "group", group, "workers", cfg.Workers)
	<-ctx.Done()
	wg.Wait() // each worker finishes (or abandons, uncommitted) its current message first
	log.Info("stopped cleanly")
}
