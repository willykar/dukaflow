// stock-watch reads pos.sales in its OWN consumer group, tracks stock, and publishes low-stock alerts.
package main

import "log"

func main() {
	// TODO (phase 2.5):
	//  1. kafka.NewReader{GroupID: "stock-watch"}: a second group receives every sale independently.
	//  2. Keep stock per (shop_id, sku); subtract each sale's qty.
	//  3. When stock falls below products.reorder_level, publish to pos.alerts (key = shop_id).
	log.Println("stock-watch: not implemented yet; see IMPLEMENTATION.md phase 2.5")
}
