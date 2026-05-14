package main
package main

import "time"

const (
	TaskQueue        = "accumulator-task-queue"
	WorkflowIDPrefix = "accumulator"
)

// maxAwaitTime is the sliding inactivity window. Use minutes (e.g. 30) in production.
var maxAwaitTime = 10 * time.Second

// OrderItem is a single event signal payload. ItemID is the deduplication key.
type OrderItem struct {
	OrderID string `json:"order_id"`
	ItemID  string `json:"item_id"`
	Name    string `json:"name"`
}
