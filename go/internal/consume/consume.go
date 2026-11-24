// Package consume is the delivery-guarantee engine shared by every DukaFlow consumer.
//
// For each message, in partition order:
//  1. handle it;
//  2. success           -> commit the offset;
//     permanent failure -> dead-letter it, then commit (one bad message never blocks the partition);
//     transient failure -> retry with capped backoff, WITHOUT committing (e.g. Postgres is down:
//     the consumer pauses instead of losing or skipping data).
//
// Committing only after the handler succeeds gives at-least-once delivery; handlers are idempotent
// (keyed on receipt_id), which makes the end result exactly-once.
package consume

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type Fetcher interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type DeadLetterer interface {
	PublishDLQFrom(ctx context.Context, m kafka.Message, reason string) error
}

// Handler processes one message. Wrap errors that can never succeed with Permanent.
type Handler func(ctx context.Context, m kafka.Message) error

type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent marks an error as one that retrying cannot fix.
func Permanent(err error) error { return permanentError{err} }

func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// Backoff returns the wait before retry number attempt (0-based): 100ms doubling, capped at 30s.
func Backoff(attempt int) time.Duration {
	d := 100 * time.Millisecond
	for i := 0; i < attempt && d < 30*time.Second; i++ {
		d *= 2
	}
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

type Loop struct {
	Fetch   Fetcher
	DLQ     DeadLetterer
	Handle  Handler
	Log     *slog.Logger
	Backoff func(attempt int) time.Duration
	// OnResult is an optional hook for metrics: "ok", "dead_lettered" or "retry".
	OnResult func(result string)
}

// Run processes messages until ctx is cancelled. It returns nil on a clean shutdown.
func (l Loop) Run(ctx context.Context) error {
	backoff := l.Backoff
	if backoff == nil {
		backoff = Backoff
	}
	for {
		m, err := l.Fetch.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			l.Log.Error("fetch failed", "err", err)
			if !sleep(ctx, backoff(3)) {
				return nil
			}
			continue
		}
		if !l.process(ctx, m, backoff) {
			return nil // shutting down mid-retry: the offset stays uncommitted, so it is redelivered
		}
		if !l.retry(ctx, backoff, "commit", func() error { return l.Fetch.CommitMessages(ctx, m) }) {
			return nil
		}
	}
}

// process returns false only if ctx was cancelled before the message was settled.
func (l Loop) process(ctx context.Context, m kafka.Message, backoff func(int) time.Duration) bool {
	for attempt := 0; ; attempt++ {
		err := l.Handle(ctx, m)
		switch {
		case err == nil:
			l.result("ok")
			return true
		case IsPermanent(err):
			l.Log.Warn("dead-lettering message", "err", err, "partition", m.Partition, "offset", m.Offset)
			ok := l.retry(ctx, backoff, "dead-letter", func() error { return l.DLQ.PublishDLQFrom(ctx, m, err.Error()) })
			if ok {
				l.result("dead_lettered")
			}
			return ok
		default:
			l.result("retry")
			l.Log.Warn("transient failure, retrying", "err", err, "attempt", attempt+1, "offset", m.Offset)
			if !sleep(ctx, backoff(attempt)) {
				return false
			}
		}
	}
}

func (l Loop) retry(ctx context.Context, backoff func(int) time.Duration, what string, fn func() error) bool {
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		l.Log.Error(what+" failed, retrying", "err", err, "attempt", attempt+1)
		if !sleep(ctx, backoff(attempt)) {
			return false
		}
	}
}

func (l Loop) result(r string) {
	if l.OnResult != nil {
		l.OnResult(r)
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
