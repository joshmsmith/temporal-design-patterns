package main

import (
	"context"
	"time"
)

func ProcessRecord(_ context.Context, recordID int) error {
	// Simulate processing work.
	time.Sleep(50 * time.Millisecond)
	return nil
}
