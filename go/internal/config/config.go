// Package config reads every service's settings from the environment, with defaults that match
// docker-compose.yml, so `go run` works locally with no setup.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Brokers     []string
	TopicSales  string
	TopicDLQ    string
	TopicAlerts string
	PostgresURL string
	GatewayAddr string
	MetricsAddr string
	Workers     int
	SweepEvery  time.Duration
	AuthCache   time.Duration
}

func FromEnv() Config {
	return Config{
		Brokers:     strings.Split(get("KAFKA_BROKERS", "localhost:9092"), ","),
		TopicSales:  get("KAFKA_TOPIC_SALES", "pos.sales"),
		TopicDLQ:    get("KAFKA_TOPIC_DLQ", "pos.sales.dlq"),
		TopicAlerts: get("KAFKA_TOPIC_ALERTS", "pos.alerts"),
		PostgresURL: get("POSTGRES_URL", "postgresql://duka:duka@localhost:5433/dukaflow"),
		GatewayAddr: get("GATEWAY_ADDR", ":8080"),
		MetricsAddr: get("METRICS_ADDR", ""),
		Workers:     getInt("CONSUMER_WORKERS", 3),
		SweepEvery:  getDuration("STOCK_SWEEP_EVERY", 30*time.Second),
		AuthCache:   getDuration("AUTH_CACHE_TTL", time.Minute),
	}
}

func get(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func getInt(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil && n > 0 {
		return n
	}
	return def
}

func getDuration(key string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil && d > 0 {
		return d
	}
	return def
}
