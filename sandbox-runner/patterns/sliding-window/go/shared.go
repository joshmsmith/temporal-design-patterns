package main

import "fmt"

const (
	TaskQueue        = "sliding-window-task-queue"
	WorkflowIDPrefix = "sliding-window"
	WindowSize       = 3
	CompletionSignal = "recordCompleted"
)

var RecordIDs = func() []string {
	ids := make([]string, 12)
	for i := range ids {
		ids[i] = fmt.Sprintf("record-%d", i)
	}
	return ids
}()

// SlidingWindowInput is the single input argument for SlidingWindowWorkflow.
// Bundling all fields into one struct keeps the ContinueAsNew call shape
// consistent across every run.
type SlidingWindowInput struct {
	RecordIDs      []string
	WindowSize     int
	StartIndex     int
	TotalProcessed int
	InFlight       int
}
