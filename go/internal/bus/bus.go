// Package bus is the Kafka side of DukaFlow: one producer for sales, dead letters and alerts, plus
// topic creation so a fresh cluster is ready without manual commands.
package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/willykar/dukaflow/go/internal/sale"
)

type Topics struct {
	Sales  string
	DLQ    string
	Alerts string
}

// Header names on dead-lettered messages, so anyone reading the DLQ can see why and where from.
const (
	HeaderReason          = "dlq-reason"
	HeaderSourceTopic     = "dlq-source-topic"
	HeaderSourcePartition = "dlq-source-partition"
	HeaderSourceOffset    = "dlq-source-offset"
	HeaderFailedAt        = "dlq-failed-at"
)

// Alert is what lands on the alerts topic and, later, on the dashboard.
type Alert struct {
	Type         string    `json:"type"` // "low_stock"
	ShopID       int       `json:"shop_id"`
	SKU          string    `json:"sku"`
	OnHand       int       `json:"on_hand"`
	ReorderLevel int       `json:"reorder_level"`
	At           time.Time `json:"at"`
}

type Producer struct {
	w      *kafka.Writer
	topics Topics
}

// NewProducer writes synchronously and waits for every in-sync replica (acks=all), so a sale is only
// acknowledged to the till once Kafka has durably stored it. Messages are partitioned by key hash.
func NewProducer(brokers []string, topics Topics) *Producer {
	return &Producer{
		topics: topics,
		w: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireAll,
			BatchTimeout:           5 * time.Millisecond, // low latency: a till waits on this
			WriteTimeout:           10 * time.Second,
			AllowAutoTopicCreation: false,
		},
	}
}

func (p *Producer) Close() error { return p.w.Close() }

// PublishSale keys by shop so one shop's sales stay ordered within a single partition.
func (p *Producer) PublishSale(ctx context.Context, s sale.Sale) error {
	body, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return p.w.WriteMessages(ctx, kafka.Message{
		Topic:   p.topics.Sales,
		Key:     []byte(strconv.Itoa(s.ShopID)),
		Value:   body,
		Headers: []kafka.Header{{Key: "receipt-id", Value: []byte(s.ReceiptID)}},
	})
}

// PublishDLQ dead-letters a payload that never made it onto the sales topic (rejected at the gateway).
func (p *Producer) PublishDLQ(ctx context.Context, payload []byte, reason string) error {
	return p.w.WriteMessages(ctx, kafka.Message{
		Topic: p.topics.DLQ,
		Value: payload,
		Headers: []kafka.Header{
			{Key: HeaderReason, Value: []byte(reason)},
			{Key: HeaderSourceTopic, Value: []byte("gateway")},
			{Key: HeaderFailedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	})
}

// PublishDLQFrom dead-letters a message read from a topic, keeping its key and exact source position
// so it can be traced and replayed.
func (p *Producer) PublishDLQFrom(ctx context.Context, m kafka.Message, reason string) error {
	return p.w.WriteMessages(ctx, kafka.Message{
		Topic: p.topics.DLQ,
		Key:   m.Key,
		Value: m.Value,
		Headers: append(append([]kafka.Header(nil), m.Headers...),
			kafka.Header{Key: HeaderReason, Value: []byte(reason)},
			kafka.Header{Key: HeaderSourceTopic, Value: []byte(m.Topic)},
			kafka.Header{Key: HeaderSourcePartition, Value: []byte(strconv.Itoa(m.Partition))},
			kafka.Header{Key: HeaderSourceOffset, Value: []byte(strconv.FormatInt(m.Offset, 10))},
			kafka.Header{Key: HeaderFailedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		),
	})
}

func (p *Producer) PublishAlert(ctx context.Context, a Alert) error {
	body, err := json.Marshal(a)
	if err != nil {
		return err
	}
	return p.w.WriteMessages(ctx, kafka.Message{Topic: p.topics.Alerts, Key: []byte(strconv.Itoa(a.ShopID)), Value: body})
}

// TopicSpecs are the partition counts DukaFlow expects. Sales: 6 partitions keyed by shop for ordering
// plus parallelism; DLQ: 1 (low volume, read in order by a human); alerts: 3.
func (t Topics) Specs() []kafka.TopicConfig {
	return []kafka.TopicConfig{
		{Topic: t.Sales, NumPartitions: 6, ReplicationFactor: 1},
		{Topic: t.DLQ, NumPartitions: 1, ReplicationFactor: 1},
		{Topic: t.Alerts, NumPartitions: 3, ReplicationFactor: 1},
	}
}

// EnsureTopics creates any missing topics. Existing topics are left alone.
func EnsureTopics(ctx context.Context, brokers []string, specs []kafka.TopicConfig) error {
	var d kafka.Dialer
	conn, err := d.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()
	ctrl, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("find controller: %w", err)
	}
	cc, err := d.DialContext(ctx, "tcp", net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer cc.Close()
	for _, spec := range specs {
		if err := cc.CreateTopics(spec); err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", spec.Topic, err)
		}
	}
	return nil
}

// Ping checks a broker answers, for readiness probes.
func Ping(ctx context.Context, brokers []string) error {
	var d kafka.Dialer
	conn, err := d.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	return conn.Close()
}
