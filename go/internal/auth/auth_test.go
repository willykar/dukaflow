package auth

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type countingStore struct {
	Static
	calls int
}

func (c *countingStore) ShopForKeyHash(ctx context.Context, h string) (int, bool, error) {
	c.calls++
	return c.Static.ShopForKeyHash(ctx, h)
}

func TestNewKeyIsRandomAndPrefixed(t *testing.T) {
	a, _ := NewKey()
	b, _ := NewKey()
	if a == b || !strings.HasPrefix(a, KeyPrefix) || len(a) != len(KeyPrefix)+64 {
		t.Fatalf("bad keys %q %q", a, b)
	}
}

func TestHashIsStableAndNotTheKey(t *testing.T) {
	if HashKey("k") != HashKey("k") || HashKey("k") == "k" || len(HashKey("k")) != 64 {
		t.Fatal("hash must be a stable 64-char hex digest")
	}
}

func TestCachedResolvesAndCaches(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := &countingStore{Static: Static{HashKey("good"): 7}}
	c := &Cached{Store: store, TTL: time.Minute, Now: func() time.Time { return now }}

	for i := 0; i < 3; i++ {
		if shop, ok, _ := c.ShopForKey(context.Background(), "good"); !ok || shop != 7 {
			t.Fatalf("got shop=%d ok=%v", shop, ok)
		}
	}
	if store.calls != 1 {
		t.Fatalf("store called %d times, want 1 (cached)", store.calls)
	}
	now = now.Add(2 * time.Minute)
	c.ShopForKey(context.Background(), "good")
	if store.calls != 2 {
		t.Fatal("cache entry must expire after the TTL, so revocations take effect")
	}
}

func TestUnknownAndEmptyKeys(t *testing.T) {
	c := &Cached{Store: Static{}, TTL: time.Minute}
	if _, ok, _ := c.ShopForKey(context.Background(), "nope"); ok {
		t.Fatal("unknown key accepted")
	}
	if _, ok, _ := c.ShopForKey(context.Background(), ""); ok {
		t.Fatal("empty key accepted")
	}
}

type flakyStore struct {
	Static
	down bool
}

func (f *flakyStore) ShopForKeyHash(ctx context.Context, h string) (int, bool, error) {
	if f.down {
		return 0, false, errors.New("db down")
	}
	return f.Static.ShopForKeyHash(ctx, h)
}

func TestKnownKeysKeepWorkingWhenTheStoreIsDown(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := &flakyStore{Static: Static{HashKey("good"): 7}}
	c := &Cached{Store: store, TTL: time.Minute, Now: func() time.Time { return now }}
	c.ShopForKey(context.Background(), "good") // learned while the store was up

	store.down = true
	now = now.Add(10 * time.Minute) // cache entry expired
	if shop, ok, err := c.ShopForKey(context.Background(), "good"); err != nil || !ok || shop != 7 {
		t.Fatalf("known key during outage: shop=%d ok=%v err=%v, want it accepted", shop, ok, err)
	}
	if _, ok, err := c.ShopForKey(context.Background(), "never-seen"); ok || err == nil {
		t.Fatal("an unknown key during an outage must fail with an error (503), not be accepted")
	}
}
