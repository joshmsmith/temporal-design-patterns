package main

import "fmt"

const (
	TaskQueue        = "mapreduce-tree-task-queue"
	WorkflowIDPrefix = "mapreduce-tree"
	LeafThreshold    = 3
	MaxDepth         = 5
	ResultSignal     = "nodeResult"
)

// Records is the demo record set.
var Records = func() []string {
	r := make([]string, 9)
	for i := range r {
		r[i] = fmt.Sprintf("item-%d", i)
	}
	return r
}()

// ResultPayload is the signal payload sent from child to parent.
type ResultPayload struct {
	ID      string   `json:"id"`
	Results []string `json:"results"`
}
