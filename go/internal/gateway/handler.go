// Package gateway is the HTTP entry point tills post sales to.
package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/willykar/dukaflow/go/internal/sale"
)

// Publisher sends a validated sale to Kafka (pos.sales) or a rejected payload to the DLQ.
// Implemented with segmentio/kafka-go in phase 2.3; tests use a fake.
type Publisher interface {
	PublishSale(ctx context.Context, s sale.Sale) error
	PublishDLQ(ctx context.Context, payload []byte, reason string) error
}

type Handler struct {
	Pub Publisher
	Now func() time.Time
}

const maxBody = 1 << 20 // 1 MiB: no sale is that large

func (h Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /v1/sales", h.postSale)
	return mux
}

func (h Handler) postSale(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, maxBody)
	var raw json.RawMessage
	if err := json.NewDecoder(body).Decode(&raw); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	var s sale.Sale
	if err := json.Unmarshal(raw, &s); err != nil {
		h.reject(w, r, raw, "decode: "+err.Error())
		return
	}
	if err := s.Validate(h.Now()); err != nil {
		h.reject(w, r, raw, err.Error())
		return
	}
	if err := h.Pub.PublishSale(r.Context(), s); err != nil {
		http.Error(w, "could not queue sale", http.StatusServiceUnavailable) // the till retries; receipt_id dedupes
		return
	}
	w.WriteHeader(http.StatusAccepted) // queued, not yet stored
}

// reject sends the payload to the DLQ so nothing is silently lost, then tells the till it was bad.
func (h Handler) reject(w http.ResponseWriter, r *http.Request, raw []byte, reason string) {
	_ = h.Pub.PublishDLQ(r.Context(), raw, reason)
	http.Error(w, reason, http.StatusBadRequest)
}
