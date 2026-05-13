package main

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// LeafWorkflow processes one record via an Activity and signals the result
// back to the parent Node workflow.
func LeafWorkflow(ctx workflow.Context, record string, parentWorkflowID string) error {
	ao := workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var result string
	if err := workflow.ExecuteActivity(ctx, ProcessLeaf, record).Get(ctx, &result); err != nil {
		return err
	}
	workflow.GetLogger(ctx).Info("Leaf processed", "record", record, "result", result)

	payload := ResultPayload{ID: record, Results: []string{result}}
	return workflow.SignalExternalWorkflow(ctx, parentWorkflowID, "", ResultSignal, payload).Get(ctx, nil)
}

// NodeWorkflow recursively splits the record set or fans out to leaf workflows,
// collects results via signals, then signals aggregated results up to its parent.
func NodeWorkflow(ctx workflow.Context, records []string, depth int, parentWorkflowID string) ([]string, error) {
	if depth > MaxDepth {
		return nil, fmt.Errorf("tree depth exceeded %d", MaxDepth)
	}

	myID := workflow.GetInfo(ctx).WorkflowExecution.ID
	resultCh := workflow.GetSignalChannel(ctx, ResultSignal)

	var collected []string
	expected := 0

	if len(records) <= LeafThreshold {
		// Fan out to leaf workflows — one per record.
		expected = len(records)
		for _, record := range records {
			cwo := workflow.ChildWorkflowOptions{
				WorkflowID: myID + "/leaf-" + record,
				TaskQueue:  TaskQueue,
			}
			workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, cwo), LeafWorkflow, record, myID)
		}
	} else {
		// Split and recurse into child node workflows.
		mid := len(records) / 2
		chunks := [][]string{records[:mid], records[mid:]}
		expected = len(chunks)
		for i, chunk := range chunks {
			cwo := workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("%s/node-d%d-%d", myID, depth+1, i),
				TaskQueue:  TaskQueue,
			}
			workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, cwo), NodeWorkflow, chunk, depth+1, myID)
		}
	}

	// Collect all expected signals.
	for i := 0; i < expected; i++ {
		var payload ResultPayload
		resultCh.Receive(ctx, &payload)
		collected = append(collected, payload.Results...)
	}

	workflow.GetLogger(ctx).Info("Node complete",
		"depth", depth,
		"records", len(records),
		"results", len(collected))

	// Signal aggregated results up to parent (if this is not the root).
	if parentWorkflowID != "" {
		payload := ResultPayload{ID: myID, Results: collected}
		if err := workflow.SignalExternalWorkflow(ctx, parentWorkflowID, "", ResultSignal, payload).Get(ctx, nil); err != nil {
			return collected, err
		}
	}

	return collected, nil
}
