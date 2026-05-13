package main

import (
	"fmt"
	"strings"
	"time"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
)

// RecordProcessorWorkflow is the child workflow that processes one record
// and signals the parent on completion.
func RecordProcessorWorkflow(ctx workflow.Context, recordID string, parentWorkflowID string) error {
	ao := workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}
	ctx = workflow.WithActivityOptions(ctx, ao)

	if err := workflow.ExecuteActivity(ctx, ProcessRecord, recordID).Get(ctx, nil); err != nil {
		return err
	}
	workflow.GetLogger(ctx).Info("Processed record", "recordID", recordID)

	// Signal the parent that this slot is now free.
	// Ignore if the parent has already completed (final run finished before us).
	err := workflow.SignalExternalWorkflow(ctx, parentWorkflowID, "", CompletionSignal, recordID).Get(ctx, nil)
	if err != nil && strings.Contains(err.Error(), "not found") {
		workflow.GetLogger(ctx).Info("Parent already completed, signal not needed", "recordID", recordID)
		return nil
	}
	return err
}

// SlidingWindowWorkflow is the parent workflow that maintains a fixed window of
// concurrent child workflows. It calls ContinueAsNew after dispatching windowSize
// children so history stays bounded.
func SlidingWindowWorkflow(ctx workflow.Context, input SlidingWindowInput) (int, error) {
	windowSize := input.WindowSize
	if windowSize <= 0 {
		windowSize = WindowSize
	}
	recordIDs := input.RecordIDs
	startIndex := input.StartIndex
	totalProcessed := input.TotalProcessed
	inFlight := input.InFlight
	parentID := workflow.GetInfo(ctx).WorkflowExecution.ID

	completedCh := workflow.GetSignalChannel(ctx, CompletionSignal)
	dispatched := 0
	active := inFlight

	startChild := func(recordID string) {
		cwo := workflow.ChildWorkflowOptions{
			WorkflowID:        fmt.Sprintf("%s/record-%s", parentID, recordID),
			TaskQueue:         TaskQueue,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON,
		}
		workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, cwo), RecordProcessorWorkflow, recordID, parentID)
	}

	// Only start (windowSize - inFlight) new children. Carried-over in-flight
	// children from the previous run will signal us when they complete.
	newFill := len(recordIDs) - startIndex
	if newFill > windowSize-inFlight {
		newFill = windowSize - inFlight
	}
	nextIndex := startIndex
	for i := 0; i < newFill; i++ {
		startChild(recordIDs[nextIndex])
		nextIndex++
		dispatched++
		active++
	}

	// If the window is full after the initial fill, continue-as-new immediately
	// so the parent doesn't wait before handing off to the next run.
	if dispatched >= windowSize {
		workflow.GetLogger(ctx).Info("ContinueAsNew", "nextIndex", nextIndex, "totalProcessed", totalProcessed)
		return 0, workflow.NewContinueAsNewError(ctx, SlidingWindowWorkflow, SlidingWindowInput{
			RecordIDs:      recordIDs,
			WindowSize:     windowSize,
			StartIndex:     nextIndex,
			TotalProcessed: totalProcessed,
			InFlight:       windowSize,
		})
	}

	// Slide the window.
	for nextIndex < len(recordIDs) {
		var sig string
		completedCh.Receive(ctx, &sig)
		totalProcessed++
		active--

		startChild(recordIDs[nextIndex])
		nextIndex++
		dispatched++
		active++

		if dispatched >= windowSize {
			workflow.GetLogger(ctx).Info("ContinueAsNew", "nextIndex", nextIndex, "totalProcessed", totalProcessed)
			// Pass nextIndex as the next unstarted record; inFlight=windowSize because
			// the window is always full at CAN time.
			return 0, workflow.NewContinueAsNewError(ctx, SlidingWindowWorkflow, SlidingWindowInput{
				RecordIDs:      recordIDs,
				WindowSize:     windowSize,
				StartIndex:     nextIndex,
				TotalProcessed: totalProcessed,
				InFlight:       windowSize,
			})
		}
	}

	// Drain all remaining in-flight children.
	for active > 0 {
		completedCh.Receive(ctx, nil)
		totalProcessed++
		active--
	}
	workflow.GetLogger(ctx).Info("Sliding window complete", "total", len(recordIDs), "totalProcessed", totalProcessed)
	return totalProcessed, nil
}
