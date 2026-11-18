// sales-consumer reads pos.sales (consumer group sales-writer) and writes each sale to PostgreSQL.
package main

import (
	"log"

	"github.com/willykar/dukaflow/go/internal/store"
)

func main() {
	_ = store.InsertSaleSQL
	// TODO (phase 2.4):
	//  1. kafka.NewReader{GroupID: "sales-writer"}; use FetchMessage so you decide when to commit.
	//  2. Decode + sale.Validate; on failure -> pos.sales.dlq, then CommitMessages, continue.
	//  3. store.SaveSale in one transaction; only after it succeeds -> CommitMessages.
	//  4. One goroutine per partition keeps each shop's sales in order.
	log.Println("sales-consumer: not implemented yet; see IMPLEMENTATION.md phase 2.4")
}
