// Package auth turns a till's API key into the one shop it may post sales for.
//
// Keys are random, shown once at creation, and stored only as SHA-256 hashes: a database leak does not
// leak usable keys. Lookups are cached briefly so the gateway does not hit Postgres on every sale.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

const KeyPrefix = "dk_"

// NewKey returns a fresh random key, e.g. dk_3f9c... (32 random bytes, hex encoded).
func NewKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return KeyPrefix + hex.EncodeToString(b), nil
}

// HashKey is what the database stores and what lookups use.
func HashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Store looks up an active (not revoked) key by its hash.
type Store interface {
	ShopForKeyHash(ctx context.Context, keyHash string) (shopID int, ok bool, err error)
}

// Resolver is what the gateway needs.
type Resolver interface {
	ShopForKey(ctx context.Context, key string) (shopID int, ok bool, err error)
}

// Cached wraps a Store with a TTL cache. Revoking a key takes effect within one TTL while the store is
// reachable; during a store outage, keys that were valid keep working (see ShopForKey).
type Cached struct {
	Store Store
	TTL   time.Duration
	Now   func() time.Time

	mu      sync.Mutex
	entries map[string]entry
}

type entry struct {
	shopID  int
	ok      bool
	expires time.Time
}

func (c *Cached) ShopForKey(ctx context.Context, key string) (int, bool, error) {
	if key == "" {
		return 0, false, nil
	}
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	h := HashKey(key)
	c.mu.Lock()
	e, found := c.entries[h]
	c.mu.Unlock()
	if found && now().Before(e.expires) {
		return e.shopID, e.ok, nil
	}

	shopID, ok, err := c.Store.ShopForKeyHash(ctx, h)
	if err != nil {
		// Fail static: if the key store is down but this key was valid last time we checked, keep
		// accepting it. Sales still reach Kafka (and are stored once the database is back) instead of
		// being refused. The cost is that a revocation waits until the store is reachable again.
		if found && e.ok {
			return e.shopID, true, nil
		}
		return 0, false, err // unknown key and no store: the gateway answers 503 and the till retries
	}
	c.mu.Lock()
	if c.entries == nil {
		c.entries = map[string]entry{}
	}
	c.entries[h] = entry{shopID: shopID, ok: ok, expires: now().Add(c.TTL)}
	c.mu.Unlock()
	return shopID, ok, nil
}

// Static is an in-memory Store for tests and demos.
type Static map[string]int // key hash -> shop id

func (s Static) ShopForKeyHash(_ context.Context, h string) (int, bool, error) {
	id, ok := s[h]
	return id, ok, nil
}
