package main

import (
	"context"
	"fmt"
	"log"
	"strings"
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
		NodeWorkflow,
		Records,
		0,
		"",
	)
	if err != nil {
		log.Fatalln("Unable to execute workflow:", err)
	}
	fmt.Printf("Started workflow: %s\n", we.GetID())
	fmt.Printf("Processing %d records via MapReduce Tree…\n", len(Records))

	var results []string
	if err := we.Get(context.Background(), &results); err != nil {
		log.Fatalln("Workflow failed:", err)
	}
	fmt.Printf("MapReduce Tree complete: %d results\n", len(results))
	fmt.Printf("Results: %s\n", strings.Join(results, ", "))
	fmt.Printf(
		"Open the Temporal UI and search for '%s' to see the full workflow tree.\n",
		workflowID,
	)
}
