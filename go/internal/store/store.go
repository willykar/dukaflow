// Package store is DukaFlow's PostgreSQL access: saving sales, API keys, stock levels and alerts.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/willykar/dukaflow/go/internal/sale"
)

// ErrTillNotInShop means the sale names a till that belongs to a different shop (or no shop).
var ErrTillNotInShop = errors.New("till does not belong to the sale's shop")

type PG struct{ Pool *pgxpool.Pool }

func Connect(ctx context.Context, url string) (*PG, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &PG{Pool: pool}, nil
}

func (p *PG) Close()                         { p.Pool.Close() }
func (p *PG) Ping(ctx context.Context) error { return p.Pool.Ping(ctx) }

// IsPermanent reports errors that retrying can never fix: bad references or bad values. Those messages
// belong in the dead-letter topic. Anything else (connection lost, timeout, deadlock) is transient.
func IsPermanent(err error) bool {
	if errors.Is(err, ErrTillNotInShop) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code[:2] {
		case "22", "23": // data exception, integrity constraint violation
			return true
		}
	}
	return false
}

// SaveSale writes the sale, its lines and its stock movements in ONE transaction.
// inserted=false means the receipt was already stored (a till retry or a Kafka redelivery): nothing
// is written twice. The caller commits its Kafka offset only after this returns nil.
func (p *PG) SaveSale(ctx context.Context, s sale.Sale) (inserted bool, err error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) // no-op after Commit

	var tillShop int
	err = tx.QueryRow(ctx, `SELECT shop_id FROM duka.tills WHERE till_id = $1`, s.TillID).Scan(&tillShop)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && tillShop != s.ShopID) {
		return false, ErrTillNotInShop
	} else if err != nil {
		return false, err
	}

	var id string
	err = tx.QueryRow(ctx, `
		INSERT INTO duka.sales (receipt_id, till_id, sold_at, synced_at, payment_method, customer_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (receipt_id) DO NOTHING
		RETURNING receipt_id`,
		s.ReceiptID, s.TillID, s.SoldAt, s.SyncedAt, s.PaymentMethod, s.CustomerID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, tx.Commit(ctx) // duplicate: already stored, nothing to do
	}
	if err != nil {
		return false, err
	}

	batch := &pgx.Batch{}
	for n, it := range s.Items {
		batch.Queue(`INSERT INTO duka.sale_items (receipt_id, line_no, sku, qty, unit_price_cents)
			VALUES ($1, $2, $3, $4, $5)`, s.ReceiptID, n+1, it.SKU, it.Qty, it.UnitPriceCents)
		batch.Queue(`INSERT INTO duka.stock_movements (shop_id, sku, moved_at, qty_change, reason, receipt_id)
			VALUES ($1, $2, $3, $4, 'sale', $5)`, s.ShopID, it.SKU, s.SoldAt, -it.Qty, s.ReceiptID)
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// ShopForKeyHash implements auth.Store.
func (p *PG) ShopForKeyHash(ctx context.Context, keyHash string) (int, bool, error) {
	var shop int
	err := p.Pool.QueryRow(ctx,
		`SELECT shop_id FROM duka.api_keys WHERE key_hash = $1 AND revoked_at IS NULL`, keyHash).Scan(&shop)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return shop, err == nil, err
}

func (p *PG) CreateKey(ctx context.Context, keyHash string, shopID int, label string) error {
	_, err := p.Pool.Exec(ctx,
		`INSERT INTO duka.api_keys (key_hash, shop_id, label) VALUES ($1, $2, $3)`, keyHash, shopID, label)
	return err
}

// RevokeKey revokes by hash; returns false if no active key matched.
func (p *PG) RevokeKey(ctx context.Context, keyHash string) (bool, error) {
	tag, err := p.Pool.Exec(ctx,
		`UPDATE duka.api_keys SET revoked_at = now() WHERE key_hash = $1 AND revoked_at IS NULL`, keyHash)
	return tag.RowsAffected() == 1, err
}

// Level is one shop's stock of one product.
type Level struct {
	ShopID       int
	SKU          string
	OnHand       int
	ReorderLevel int
}

// Levels returns current stock for the given products in one shop.
func (p *PG) Levels(ctx context.Context, shopID int, skus []string) ([]Level, error) {
	return p.levels(ctx, `SELECT shop_id, sku, on_hand, reorder_level FROM duka.stock_levels
		WHERE shop_id = $1 AND sku = ANY($2)`, shopID, skus)
}

// LowStock returns every product at or below its reorder level, in every shop (the periodic sweep).
func (p *PG) LowStock(ctx context.Context) ([]Level, error) {
	return p.levels(ctx, `SELECT shop_id, sku, on_hand, reorder_level FROM duka.stock_levels
		WHERE on_hand <= reorder_level`)
}

func (p *PG) levels(ctx context.Context, sql string, args ...any) ([]Level, error) {
	rows, err := p.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Level, error) {
		var l Level
		err := r.Scan(&l.ShopID, &l.SKU, &l.OnHand, &l.ReorderLevel)
		return l, err
	})
}

// OpenAlert opens a low-stock alert unless one is already open for that shop and product.
// opened=false means it was already open: the owner is not alerted twice.
func (p *PG) OpenAlert(ctx context.Context, l Level) (opened bool, err error) {
	tag, err := p.Pool.Exec(ctx, `
		INSERT INTO duka.stock_alerts (shop_id, sku, on_hand, reorder_level)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (shop_id, sku) WHERE cleared_at IS NULL DO NOTHING`,
		l.ShopID, l.SKU, l.OnHand, l.ReorderLevel)
	return tag.RowsAffected() == 1, err
}

// ClearRecovered closes open alerts whose stock is back above the reorder level (after a delivery).
func (p *PG) ClearRecovered(ctx context.Context) (int64, error) {
	tag, err := p.Pool.Exec(ctx, `
		UPDATE duka.stock_alerts a SET cleared_at = now()
		FROM duka.stock_levels s
		WHERE a.cleared_at IS NULL AND s.shop_id = a.shop_id AND s.sku = a.sku AND s.on_hand > s.reorder_level`)
	return tag.RowsAffected(), err
}
