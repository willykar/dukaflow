// till-gateway receives sales from tills over HTTP and queues them on Kafka.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/willykar/dukaflow/go/internal/gateway"
	"github.com/willykar/dukaflow/go/internal/sale"
)

// logPublisher stands in for Kafka until phase 2.3. Replace it with a segmentio/kafka-go writer:
// pos.sales keyed by shop_id, and pos.sales.dlq for rejects.
type logPublisher struct{}

func (logPublisher) PublishSale(_ context.Context, s sale.Sale) error {
	log.Printf("queued receipt=%s shop=%d total=%d late=%s", s.ReceiptID, s.ShopID, s.TotalCents(), s.Lateness())
	return nil
}

func (logPublisher) PublishDLQ(_ context.Context, payload []byte, reason string) error {
	log.Printf("DLQ (%s): %s", reason, payload)
	return nil
}

func main() {
	addr := os.Getenv("GATEWAY_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	h := gateway.Handler{Pub: logPublisher{}, Now: time.Now}
	srv := &http.Server{Addr: addr, Handler: h.Routes(), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("till-gateway listening on %s", addr)
	log.Fatal(srv.ListenAndServe())
}
