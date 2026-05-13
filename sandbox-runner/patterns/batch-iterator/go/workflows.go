package main

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// BatchIteratorWorkflow processes PageSize records per run, then calls
// ContinueAsNew with the next offset so history stays bounded.
func BatchIteratorWorkflow(ctx workflow.Context, offset int, totalProcessed int) (int, error) {
	ao := workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var page []int
	if err := workflow.ExecuteActivity(ctx, FetchPage, offset, PageSize).Get(ctx, &page); err != nil {
		return totalProcessed, err
	}

	for _, recordID := range page {
		if err := workflow.ExecuteActivity(ctx, ProcessRecord, recordID).Get(ctx, nil); err != nil {
			return totalProcessed, err
		}
		totalProcessed++
	}

	workflow.GetLogger(ctx).Info("Processed page",
		"offset", offset,
		"pageSize", len(page),
		"totalProcessed", totalProcessed)

	if len(page) == PageSize {
		// More pages remain — continue as new with the next offset.
		return totalProcessed, workflow.NewContinueAsNewError(ctx, BatchIteratorWorkflow, offset+PageSize, totalProcessed)
	}

	// Reached here only on the final (partial) page.
	return totalProcessed, nil
}
