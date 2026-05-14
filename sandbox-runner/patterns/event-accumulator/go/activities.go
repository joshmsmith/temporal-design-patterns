package main
package main

import (
	"context"
	"fmt"
	"strings"
)

// ProcessItems is the batch activity that processes all accumulated items together.
func ProcessItems(_ context.Context, orderID string, items []OrderItem) (string, error) {
	names := make([]string, len(items))
	for i, item := range items {
		names[i] = item.Name
	}
	return fmt.Sprintf("Order %s fulfilled with %d item(s): %s",
		orderID, len(items), strings.Join(names, ", ")), nil
}
