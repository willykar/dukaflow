// Package metrics is a tiny, dependency-free counter registry that serves the Prometheus text format,
// so any Prometheus or Grafana Agent can scrape the services without extra libraries.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
)

type Registry struct {
	mu       sync.Mutex
	counters map[string]*Counter
}

type Counter struct {
	help string
	v    atomic.Int64
}

func (c *Counter) Inc()         { c.v.Add(1) }
func (c *Counter) Add(n int64)  { c.v.Add(n) }
func (c *Counter) Value() int64 { return c.v.Load() }

func New() *Registry { return &Registry{counters: map[string]*Counter{}} }

// Counter returns the named counter, creating it on first use. Names should end in _total.
func (r *Registry) Counter(name, help string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		return c
	}
	c := &Counter{help: help}
	r.counters[name] = c
	return c
}

// ServeHTTP writes every counter in the Prometheus exposition format.
func (r *Registry) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	names := make([]string, 0, len(r.counters))
	for n := range r.counters {
		names = append(names, n)
	}
	r.mu.Unlock()
	sort.Strings(names)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, n := range names {
		c := r.Counter(n, "")
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", n, c.help, n, n, c.Value())
	}
}
