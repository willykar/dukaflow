// admin is the operator's tool.
//
//	go run ./cmd/admin create-key -shop 1 -label "till 11"   # prints the key ONCE
//	go run ./cmd/admin revoke-key -key dk_...
//	go run ./cmd/admin topics                                # create Kafka topics
//	go run ./cmd/admin dlq [-max 20]                         # show dead-lettered messages and why
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/willykar/dukaflow/go/internal/auth"
	"github.com/willykar/dukaflow/go/internal/bus"
	"github.com/willykar/dukaflow/go/internal/config"
	"github.com/willykar/dukaflow/go/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cfg := config.FromEnv()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var err error
	switch os.Args[1] {
	case "create-key":
		err = createKey(ctx, cfg, os.Args[2:])
	case "revoke-key":
		err = revokeKey(ctx, cfg, os.Args[2:])
	case "topics":
		t := bus.Topics{Sales: cfg.TopicSales, DLQ: cfg.TopicDLQ, Alerts: cfg.TopicAlerts}
		if err = bus.EnsureTopics(ctx, cfg.Brokers, t.Specs()); err == nil {
			fmt.Println("topics ready:", cfg.TopicSales, cfg.TopicDLQ, cfg.TopicAlerts)
		}
	case "dlq":
		err = showDLQ(ctx, cfg, os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: admin create-key -shop N [-label TEXT] | revoke-key -key KEY | topics | dlq [-max N]")
	os.Exit(2)
}

func createKey(ctx context.Context, cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("create-key", flag.ExitOnError)
	shop := fs.Int("shop", 0, "shop id the key may post sales for")
	label := fs.String("label", "till", "who holds the key, e.g. \"till 11\"")
	fs.Parse(args)
	if *shop <= 0 {
		return errors.New("-shop is required")
	}
	db, err := store.Connect(ctx, cfg.PostgresURL)
	if err != nil {
		return err
	}
	defer db.Close()
	key, err := auth.NewKey()
	if err != nil {
		return err
	}
	if err := db.CreateKey(ctx, auth.HashKey(key), *shop, *label); err != nil {
		return err
	}
	fmt.Println(key)
	fmt.Fprintln(os.Stderr, "Store this key now: only its hash is kept, so it cannot be shown again.")
	return nil
}

func revokeKey(ctx context.Context, cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("revoke-key", flag.ExitOnError)
	key := fs.String("key", "", "the key to revoke")
	fs.Parse(args)
	db, err := store.Connect(ctx, cfg.PostgresURL)
	if err != nil {
		return err
	}
	defer db.Close()
	ok, err := db.RevokeKey(ctx, auth.HashKey(*key))
	if err == nil && !ok {
		return errors.New("no active key matched")
	}
	if err == nil {
		fmt.Println("revoked (takes effect within the gateway's auth cache TTL)")
	}
	return err
}

// showDLQ reads the dead-letter topic from the start without joining a consumer group, so it never
// moves anyone's offsets. It stops after -max messages or when the topic goes quiet.
func showDLQ(ctx context.Context, cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("dlq", flag.ExitOnError)
	max := fs.Int("max", 20, "messages to show")
	fs.Parse(args)
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: cfg.Brokers, Topic: cfg.TopicDLQ, Partition: 0, MaxWait: time.Second})
	defer r.Close()
	if err := r.SetOffset(kafka.FirstOffset); err != nil {
		return err
	}
	shown := 0
	for shown < *max {
		readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		m, err := r.ReadMessage(readCtx)
		cancel()
		if err != nil {
			break // quiet for 3s: we have read everything
		}
		h := map[string]string{}
		for _, hd := range m.Headers {
			h[hd.Key] = string(hd.Value)
		}
		fmt.Printf("#%d reason=%q from=%s partition=%s offset=%s at=%s\n   %s\n", m.Offset,
			h[bus.HeaderReason], h[bus.HeaderSourceTopic], h[bus.HeaderSourcePartition], h[bus.HeaderSourceOffset],
			h[bus.HeaderFailedAt], truncate(string(m.Value), 160))
		shown++
	}
	fmt.Printf("%d dead-lettered message(s) shown\n", shown)
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
