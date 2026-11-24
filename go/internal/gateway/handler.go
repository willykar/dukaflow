// Package gateway is the HTTP entry point tills post sales to.
//
// Contract with the till:
//   - 202 Accepted: the sale is durably on Kafka; the till can forget it.
//   - 400 Bad Request: the sale is invalid; it was dead-lettered for inspection. Do NOT retry.
//   - 401 / 403: missing or wrong API key, or a key used for another shop's sale. Do NOT retry.
//   - 503 Service Unavailable: we could not queue it. RETRY with the same receipt_id; duplicates are
//     discarded downstream, so retrying is always safe.
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/willykar/dukaflow/go/internal/auth"
	"github.com/willykar/dukaflow/go/internal/metrics"
	"github.com/willykar/dukaflow/go/internal/sale"
)

// Publisher sends a validated sale to the sales topic, or a rejected payload to the dead-letter topic.
type Publisher interface {
	PublishSale(ctx context.Context, s sale.Sale) error
	PublishDLQ(ctx context.Context, payload []byte, reason string) error
}

type Handler struct {
	Pub     Publisher
	Auth    auth.Resolver
	Now     func() time.Time
	Log     *slog.Logger
	Metrics *metrics.Registry
	// Ready reports whether dependencies (Kafka, the key store) are reachable. Nil means always ready.
	Ready func(ctx context.Context) error
}

const maxBody = 1 << 20 // 1 MiB: no real sale is anywhere near this

func (h Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", h.readyz)
	mux.Handle("GET /metrics", h.Metrics)
	mux.HandleFunc("POST /v1/sales", h.postSale)
	return mux
}

func (h Handler) readyz(w http.ResponseWriter, r *http.Request) {
	if h.Ready != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := h.Ready(ctx); err != nil {
			http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (h Handler) count(name, help string) { h.Metrics.Counter(name, help).Inc() }

func (h Handler) postSale(w http.ResponseWriter, r *http.Request) {
	reqID := r.Header.Get("X-Request-ID")
	if reqID == "" {
		reqID = newRequestID()
	}
	w.Header().Set("X-Request-ID", reqID)
	log := h.Log.With("request_id", reqID, "remote", r.RemoteAddr)

	key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	shopID, ok, err := h.Auth.ShopForKey(r.Context(), key)
	switch {
	case err != nil:
		h.count("dukaflow_gateway_auth_errors_total", "Key lookups that failed (key store unreachable)")
		log.Error("key lookup failed", "err", err)
		http.Error(w, "temporarily unavailable, retry", http.StatusServiceUnavailable)
		return
	case !ok:
		h.count("dukaflow_gateway_unauthorized_total", "Requests with a missing, unknown or revoked key")
		http.Error(w, "missing or invalid API key", http.StatusUnauthorized)
		return
	}

	var raw json.RawMessage
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&raw); err != nil {
		h.count("dukaflow_gateway_malformed_total", "Bodies that were not JSON")
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	var s sale.Sale
	if err := json.Unmarshal(raw, &s); err != nil {
		h.reject(w, r, log, raw, "decode: "+err.Error())
		return
	}
	if s.ShopID != shopID {
		// Not a data-quality problem but a security one: never let shop A write shop B's sales.
		h.count("dukaflow_gateway_forbidden_total", "Valid keys used for another shop's sale")
		log.Warn("key used for another shop", "key_shop", shopID, "sale_shop", s.ShopID, "receipt_id", s.ReceiptID)
		http.Error(w, "this key cannot post sales for that shop", http.StatusForbidden)
		return
	}
	if err := s.Validate(h.Now()); err != nil {
		h.reject(w, r, log, raw, err.Error())
		return
	}
	if err := h.Pub.PublishSale(r.Context(), s); err != nil {
		h.count("dukaflow_gateway_publish_errors_total", "Sales we could not queue (till will retry)")
		log.Error("publish failed", "err", err, "receipt_id", s.ReceiptID)
		http.Error(w, "could not queue sale, retry", http.StatusServiceUnavailable)
		return
	}
	h.count("dukaflow_gateway_sales_accepted_total", "Sales durably queued on Kafka")
	if late := s.Lateness(); late > time.Hour {
		h.count("dukaflow_gateway_late_sales_total", "Accepted sales that synced over an hour after the sale")
	}
	w.WriteHeader(http.StatusAccepted)
}

// reject dead-letters the payload so nothing is silently lost, then tells the till not to retry.
func (h Handler) reject(w http.ResponseWriter, r *http.Request, log *slog.Logger, raw []byte, reason string) {
	h.count("dukaflow_gateway_sales_rejected_total", "Invalid sales sent to the dead-letter topic")
	if err := h.Pub.PublishDLQ(r.Context(), raw, reason); err != nil {
		h.count("dukaflow_gateway_dlq_errors_total", "Rejected sales we could not dead-letter")
		log.Error("dead-letter failed", "err", err, "reason", reason)
	}
	log.Info("sale rejected", "reason", reason)
	http.Error(w, reason, http.StatusBadRequest)
}

func newRequestID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
