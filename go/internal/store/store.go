// Package store writes sales to PostgreSQL.
package store

// The sale header and its lines are written in ONE transaction. ON CONFLICT makes a redelivered
// message harmless; the Kafka offset is committed only after the transaction commits.
const (
	InsertSaleSQL = `
INSERT INTO duka.sales (receipt_id, till_id, sold_at, synced_at, payment_method)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (receipt_id) DO NOTHING`

	InsertItemSQL = `
INSERT INTO duka.sale_items (receipt_id, line_no, sku, qty, unit_price_cents)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (receipt_id, line_no) DO NOTHING`
)

// TODO (phase 2.4): implement with github.com/jackc/pgx/v5:
//   func (s *PG) SaveSale(ctx, sale.Sale) (inserted bool, err error)
// using pool.BeginTx, both statements, and tx.Commit. inserted=false means it was a duplicate.
