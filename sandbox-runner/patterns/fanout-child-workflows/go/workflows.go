package main

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// RecordBatchWorkflow is the child workflow that processes a contiguous slice
// of records [offset, offset+length).
func RecordBatchWorkflow(ctx workflow.Context, offset int, length int) (int, error) {
	ao := workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second}
	ctx = workflow.WithActivityOptions(ctx, ao)

	processed := 0
	for i := offset; i < offset+length; i++ {
		if err := workflow.ExecuteActivity(ctx, ProcessRecord, i).Get(ctx, nil); err != nil {
			return processed, err
		}
		processed++
	}
	workflow.GetLogger(ctx).Info("Batch complete", "offset", offset, "length", length, "processed", processed)
	return processed, nil
}

// FanOutWorkflow is the parent workflow that splits the total record set into
// chunks and starts one child workflow per chunk.
func FanOutWorkflow(ctx workflow.Context, totalRecords int, chunkSize int) (int, error) {
	if chunkSize <= 0 {
		chunkSize = ChunkSize
	}

	parentID := workflow.GetInfo(ctx).WorkflowExecution.ID
	var futures []workflow.Future

	for offset := 0; offset < totalRecords; offset += chunkSize {
		length := chunkSize
		if offset+chunkSize > totalRecords {
			length = totalRecords - offset
		}
		off := offset // capture loop variable
		cwo := workflow.ChildWorkflowOptions{
			WorkflowID: fmt.Sprintf("%s/batch-%d", parentID, off),
			TaskQueue:  TaskQueue,
		}
		cctx := workflow.WithChildOptions(ctx, cwo)
		futures = append(futures, workflow.ExecuteChildWorkflow(cctx, RecordBatchWorkflow, off, length))
	}

	total := 0
	for _, f := range futures {
		var n int
		if err := f.Get(ctx, &n); err != nil {
			return total, err
		}
		total += n
	}
	workflow.GetLogger(ctx).Info("Fan-out complete",
		"totalRecords", totalRecords,
		"chunks", len(futures),
		"total", total)
	return total, nil
}
