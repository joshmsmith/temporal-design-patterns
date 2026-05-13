package main

import (
	"context"
	"time"
)

func FetchPage(_ context.Context, offset int, pageSize int) ([]int, error) {
	end := offset + pageSize
	if end > TotalRecords {
		end = TotalRecords
	}
	page := make([]int, 0, end-offset)
	for i := offset; i < end; i++ {
		page = append(page, i)
	}
	return page, nil
}

func ProcessRecord(_ context.Context, recordID int) error {
	// Simulate processing work.
	time.Sleep(50 * time.Millisecond)
	return nil
}
