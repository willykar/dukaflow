package store

// Integration tests against a real, disposable PostgreSQL. Skipped unless DUKAFLOW_TEST_DB is set.
// WARNING: they drop and recreate the `duka` schema in that database.

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/willykar/dukaflow/go/internal/sale"
)

func testDB(t *testing.T) *PG {
	t.Helper()
	url := os.Getenv("DUKAFLOW_TEST_DB")
	if url == "" {
		t.Skip("set DUKAFLOW_TEST_DB to run database tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	files, _ := filepath.Glob(filepath.Join("..", "..", "..", "sql", "postgres", "0*.sql"))
	sort.Strings(files)
	if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS duka CASCADE"); err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		sql, _ := os.ReadFile(f)
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
	}
	seed := `
		INSERT INTO duka.shops VALUES (1, 'A', 'Kasarani'), (2, 'B', 'Ruaka');
		INSERT INTO duka.tills VALUES (11, 1), (21, 2);
		INSERT INTO duka.categories VALUES (1, 'Staples');
		INSERT INTO duka.products (sku, name, category_id, reorder_level, par_level) VALUES
			('UNGA-2KG', 'Maize flour 2kg', 1, 40, 250);
		INSERT INTO duka.stock_movements (shop_id, sku, moved_at, qty_change, reason)
			VALUES (1, 'UNGA-2KG', '2026-01-01 05:00Z', 42, 'delivery');`
	if _, err := pool.Exec(ctx, seed); err != nil {
		t.Fatal(err)
	}
	return &PG{Pool: pool}
}

func testSale(receipt string, till, shop, qty int) sale.Sale {
	at := time.Date(2026, 1, 10, 11, 0, 0, 0, time.UTC)
	return sale.Sale{ReceiptID: receipt, ShopID: shop, TillID: till, SoldAt: at, SyncedAt: at.Add(time.Second),
		PaymentMethod: "cash", Items: []sale.Item{{SKU: "UNGA-2KG", Qty: qty, UnitPriceCents: 19500}}}
}

func TestSaveSaleIsIdempotentAndMovesStock(t *testing.T) {
	db, ctx := testDB(t), context.Background()
	s := testSale("5f0c6c1e-8a51-4a7e-9a55-0b1e0c2a9f11", 11, 1, 3)
	for i, want := range []bool{true, false, false} {
		got, err := db.SaveSale(ctx, s)
		if err != nil || got != want {
			t.Fatalf("save %d: inserted=%v err=%v, want inserted=%v", i, got, err, want)
		}
	}
	levels, err := db.Levels(ctx, 1, []string{"UNGA-2KG"})
	if err != nil || len(levels) != 1 || levels[0].OnHand != 39 {
		t.Fatalf("levels %+v err %v: 42 delivered - 3 sold once = 39", levels, err)
	}
}

func TestSaveSalePermanentErrors(t *testing.T) {
	db, ctx := testDB(t), context.Background()
	unknownSKU := testSale("6f0c6c1e-8a51-4a7e-9a55-0b1e0c2a9f12", 11, 1, 1)
	unknownSKU.Items[0].SKU = "NOPE"
	wrongTill := testSale("7f0c6c1e-8a51-4a7e-9a55-0b1e0c2a9f13", 21, 1, 1) // till 21 is shop 2's
	for name, s := range map[string]sale.Sale{"unknown sku": unknownSKU, "till of another shop": wrongTill} {
		_, err := db.SaveSale(ctx, s)
		if err == nil || !IsPermanent(err) {
			t.Fatalf("%s: err=%v, want a permanent error", name, err)
		}
	}
	var n int
	db.Pool.QueryRow(ctx, "SELECT count(*) FROM duka.sales").Scan(&n)
	if n != 0 {
		t.Fatal("a failed sale must leave nothing behind (one transaction)")
	}
}

func TestAlertsOpenOnceAndClearAfterRestock(t *testing.T) {
	db, ctx := testDB(t), context.Background()
	if _, err := db.SaveSale(ctx, testSale("8f0c6c1e-8a51-4a7e-9a55-0b1e0c2a9f14", 11, 1, 5)); err != nil {
		t.Fatal(err)
	}
	low, _ := db.LowStock(ctx) // 42 - 5 = 37 <= 40
	if len(low) != 1 {
		t.Fatalf("low stock %+v", low)
	}
	first, _ := db.OpenAlert(ctx, low[0])
	second, _ := db.OpenAlert(ctx, low[0])
	if !first || second {
		t.Fatalf("open alert twice: first=%v second=%v, want true then false", first, second)
	}
	db.Pool.Exec(ctx, `INSERT INTO duka.stock_movements (shop_id, sku, moved_at, qty_change, reason)
		VALUES (1, 'UNGA-2KG', '2026-01-12 05:00Z', 213, 'delivery')`)
	if n, err := db.ClearRecovered(ctx); err != nil || n != 1 {
		t.Fatalf("cleared %d err %v", n, err)
	}
	if again, _ := db.OpenAlert(ctx, low[0]); !again {
		t.Fatal("after recovery, a new drop must be able to open a new alert")
	}
}

func TestAPIKeys(t *testing.T) {
	db, ctx := testDB(t), context.Background()
	if err := db.CreateKey(ctx, "h1", 1, "till 11"); err != nil {
		t.Fatal(err)
	}
	if shop, ok, err := db.ShopForKeyHash(ctx, "h1"); err != nil || !ok || shop != 1 {
		t.Fatalf("lookup: shop=%d ok=%v err=%v", shop, ok, err)
	}
	if revoked, _ := db.RevokeKey(ctx, "h1"); !revoked {
		t.Fatal("revoke failed")
	}
	if _, ok, _ := db.ShopForKeyHash(ctx, "h1"); ok {
		t.Fatal("a revoked key must not resolve")
	}
}
