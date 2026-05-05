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

	ctx := context.Background()
	orderID := fmt.Sprintf("%d", time.Now().UnixMilli())
	workflowID := WorkflowIDPrefix + "-" + orderID

	order := OrderInput{OrderID: orderID, Amount: 99.99}
	fmt.Printf("Starting order workflow: %s\n", workflowID)

	we, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: TaskQueue,
	}, OrderWorkflow, order)
	if err != nil {
		log.Fatalln("Start workflow failed:", err)
	}

	// Simulate the inbound webhook arriving 3 seconds later.
	// In production this would be your HTTP handler calling SignalWorkflow.
	fmt.Println("Simulating inbound payment webhook in 3 seconds...")
	time.Sleep(3 * time.Second)

	payment := PaymentPayload{
		PaymentID: fmt.Sprintf("pay-%d", time.Now().UnixMilli()),
		Amount:    99.99,
	}

	// This is what your HTTP /webhook handler does upon receiving the POST:
	err = c.SignalWorkflow(ctx, workflowID, we.GetRunID(), SignalName, payment)
	if err != nil {
		log.Fatalln("Signal failed:", err)
	}
	fmt.Printf("Webhook signal sent: %s\n", payment.PaymentID)

	var result string
	if err := we.Get(ctx, &result); err != nil {
		log.Fatalln("Workflow result failed:", err)
	}
	fmt.Printf("Order completed: %s\n", result)
	fmt.Printf("Open the Temporal UI and search for '%s' to see the history.\n", workflowID)
}
