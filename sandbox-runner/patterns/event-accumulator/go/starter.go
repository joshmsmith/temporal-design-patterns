package main
package main

import (
	"context"
	"fmt"
	"log"

	"go.temporal.io/sdk/client"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "localhost:7233"})
	if err != nil {
		log.Fatalln("Unable to create client:", err)
	}
	defer c.Close()

	// Items arriving for two separate orders from multiple producers.
	// item-1 is sent twice to demonstrate deduplication.
	items := []OrderItem{
		{OrderID: "order-A", ItemID: "item-1", Name: "Widget"},
		{OrderID: "order-B", ItemID: "item-2", Name: "Gadget"},
		{OrderID: "order-A", ItemID: "item-3", Name: "Gizmo"},
		{OrderID: "order-B", ItemID: "item-4", Name: "Doohickey"},
		{OrderID: "order-A", ItemID: "item-1", Name: "Widget"}, // duplicate — ignored
		{OrderID: "order-A", ItemID: "item-5", Name: "Thingamajig"},
	}

	for _, item := range items {
		workflowID := fmt.Sprintf("%s-%s", WorkflowIDPrefix, item.OrderID)
		// Signal-With-Start: start the workflow if not running, otherwise just signal
		_, err := c.SignalWithStartWorkflow(
			context.Background(),
			workflowID,
			"add-item",
			item,
			client.StartWorkflowOptions{
				ID:        workflowID,
				TaskQueue: TaskQueue,
			},
			AccumulatorWorkflow,
			item.OrderID,
			[]OrderItem{},
			[]string{},
		)
		if err != nil {
			log.Fatalf("SignalWithStartWorkflow failed: %v", err)
		}
		fmt.Printf("Signaled %s: %s (%s)\n", workflowID, item.Name, item.ItemID)
	}

	fmt.Println("\nWaiting for workflows to process their items...")
	for _, orderID := range []string{"order-A", "order-B"} {
		workflowID := fmt.Sprintf("%s-%s", WorkflowIDPrefix, orderID)
		run := c.GetWorkflow(context.Background(), workflowID, "")
		var result string
		if err := run.Get(context.Background(), &result); err != nil {
			log.Fatalf("Workflow %s failed: %v", workflowID, err)
		}
		fmt.Printf("%s result: %s\n", workflowID, result)
	}
	fmt.Printf("\nOpen the Temporal UI and search for '%s' to see the accumulator workflows.\n", WorkflowIDPrefix)
}
