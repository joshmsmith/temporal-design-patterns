package main

import (
	"context"
	"time"
)

func ProcessRecord(_ context.Context, recordID string) error {
	// Simulate processing work.
	time.Sleep(300 * time.Millisecond)
	return nil
}
