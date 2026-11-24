package consume

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// fakeFetcher hands out queued messages, then cancels the run once they are all committed.
type fakeFetcher struct {
	msgs      []kafka.Message
	committed []int64
	cancel    context.CancelFunc
	events    *[]string
}

func (f *fakeFetcher) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if len(f.msgs) == 0 {
		f.cancel()
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	m := f.msgs[0]
	f.msgs = f.msgs[1:]
	return m, nil
}

func (f *fakeFetcher) CommitMessages(_ context.Context, ms ...kafka.Message) error {
	for _, m := range ms {
		f.committed = append(f.committed, m.Offset)
		*f.events = append(*f.events, "commit")
	}
	return nil
}

type fakeDLQ struct {
	offsets []int64
	events  *[]string
}

func (d *fakeDLQ) PublishDLQFrom(_ context.Context, m kafka.Message, _ string) error {
	d.offsets = append(d.offsets, m.Offset)
	*d.events = append(*d.events, "dlq")
	return nil
}

func run(t *testing.T, msgs []kafka.Message, h Handler) (*fakeFetcher, *fakeDLQ, []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var events []string
	f := &fakeFetcher{msgs: msgs, cancel: cancel, events: &events}
	d := &fakeDLQ{events: &events}
	loop := Loop{Fetch: f, DLQ: d, Handle: func(ctx context.Context, m kafka.Message) error {
		events = append(events, "handle")
		return h(ctx, m)
	}, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Backoff: func(int) time.Duration { return time.Millisecond }}
	if err := loop.Run(ctx); err != nil {
		t.Fatal(err)
	}
	return f, d, events
}

func msgs(offsets ...int64) []kafka.Message {
	out := make([]kafka.Message, len(offsets))
	for i, o := range offsets {
		out[i] = kafka.Message{Offset: o}
	}
	return out
}

func TestSuccessCommitsAfterHandling(t *testing.T) {
	f, d, events := run(t, msgs(1, 2), func(context.Context, kafka.Message) error { return nil })
	if len(f.committed) != 2 || len(d.offsets) != 0 {
		t.Fatalf("committed=%v dlq=%v", f.committed, d.offsets)
	}
	want := []string{"handle", "commit", "handle", "commit"}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events %v: the offset must be committed only after handling", events)
		}
	}
}

func TestPermanentFailureIsDeadLetteredThenCommitted(t *testing.T) {
	f, d, events := run(t, msgs(1, 2), func(_ context.Context, m kafka.Message) error {
		if m.Offset == 1 {
			return Permanent(errors.New("unknown sku"))
		}
		return nil
	})
	if len(d.offsets) != 1 || d.offsets[0] != 1 || len(f.committed) != 2 {
		t.Fatalf("dlq=%v committed=%v: bad message must be dead-lettered and must not block the next", d.offsets, f.committed)
	}
	if events[1] != "dlq" || events[2] != "commit" {
		t.Fatalf("events %v: dead-letter before commit", events)
	}
}

func TestTransientFailureRetriesWithoutCommitting(t *testing.T) {
	calls := 0
	f, d, events := run(t, msgs(7), func(context.Context, kafka.Message) error {
		calls++
		if calls < 3 {
			return errors.New("connection refused")
		}
		return nil
	})
	if calls != 3 || len(f.committed) != 1 || len(d.offsets) != 0 {
		t.Fatalf("calls=%d committed=%v dlq=%v", calls, f.committed, d.offsets)
	}
	if events[0] != "handle" || events[1] != "handle" || events[2] != "handle" || events[3] != "commit" {
		t.Fatalf("events %v: no commit until the handler finally succeeds", events)
	}
}

func TestShutdownMidRetryLeavesOffsetUncommitted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var events []string
	f := &fakeFetcher{msgs: msgs(1), cancel: cancel, events: &events}
	loop := Loop{Fetch: f, DLQ: &fakeDLQ{events: &events}, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Backoff: func(int) time.Duration { return time.Millisecond },
		Handle:  func(context.Context, kafka.Message) error { cancel(); return errors.New("db down") }}
	if err := loop.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.committed) != 0 {
		t.Fatal("an unprocessed message must stay uncommitted so it is redelivered after restart")
	}
}

func TestBackoffDoublesAndCaps(t *testing.T) {
	if Backoff(0) != 100*time.Millisecond || Backoff(1) != 200*time.Millisecond || Backoff(50) != 30*time.Second {
		t.Fatalf("backoff %v %v %v", Backoff(0), Backoff(1), Backoff(50))
	}
}
