package main

import (
	"context"
	"fmt"
	"time"
)

func ProcessLeaf(_ context.Context, record string) (string, error) {
	// Simulate processing and return a result string.
	time.Sleep(50 * time.Millisecond)
	return fmt.Sprintf("processed(%s)", record), nil
}
