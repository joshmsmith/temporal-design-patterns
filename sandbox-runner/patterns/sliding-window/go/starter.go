package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "localhost:7233"})
	if err != nil {
		log.Fatalln("Unable to create client:", err)
	}
	defer c.Close()

	workflowID := fmt.Sprintf("%s-%d", WorkflowIDPrefix, time.Now().UnixMilli())
	we, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: TaskQueue,
		},
		SlidingWindowWorkflow,
		SlidingWindowInput{
			RecordIDs:  RecordIDs,
			WindowSize: WindowSize,
		},
	)
	if err != nil {
		log.Fatalln("Unable to execute workflow:", err)
	}
	fmt.Printf("Started workflow: %s\n", we.GetID())
	fmt.Printf("Processing %d records with window size %d…\n", len(RecordIDs), WindowSize)

	var totalProcessed int
	if err := we.Get(context.Background(), &totalProcessed); err != nil {
		log.Fatalln("Workflow failed:", err)
	}
	fmt.Printf("Sliding window complete: processed %d records\n", totalProcessed)
	fmt.Printf(
		"Open the Temporal UI and search for '%s' to see the parent and child workflows.\n",
		workflowID,
	)
}
