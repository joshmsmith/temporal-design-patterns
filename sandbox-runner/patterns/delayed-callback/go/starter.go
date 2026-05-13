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

	// --- Pattern 1: Inbound Webhook via Signal-with-Start ---
	fmt.Println("=== Pattern 1: Inbound Webhook via Signal-with-Start ===")
	orderID := fmt.Sprintf("%d", time.Now().UnixMilli())
	workflowID := WorkflowIDPrefix + "-" + orderID

	order := OrderInput{OrderID: orderID, Amount: 99.99}
	payment := PaymentPayload{
		PaymentID: fmt.Sprintf("pay-%d", time.Now().UnixMilli()),
		Amount:    99.99,
	}

	fmt.Printf("Sending webhook for order: %s\n", workflowID)

	// Signal-with-Start: atomically starts the workflow (if not running) and
	// delivers the payment signal — this is exactly what your HTTP handler would do.
	we, err := c.SignalWithStartWorkflow(
		ctx,
		workflowID,
		SignalName,
		payment,
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: TaskQueue,
		},
		OrderWorkflow,
		order,
	)
	if err != nil {
		log.Fatalln("SignalWithStart failed:", err)
	}
	fmt.Printf("Webhook signal sent: %s\n", payment.PaymentID)

	var result string
	if err := we.Get(ctx, &result); err != nil {
		log.Fatalln("Workflow result failed:", err)
	}
	fmt.Printf("Order completed: %s\n", result)
	fmt.Printf("Search '%s' in the Temporal UI to inspect the history.\n", workflowID)

	// --- Pattern 2: Delayed Outbound Callback ---
	fmt.Println("\n=== Pattern 2: Delayed Outbound Callback ===")
	callbackID := fmt.Sprintf("%s-%d", CallbackWorkflowIDPrefix, time.Now().UnixMilli())
	callbackInput := CallbackInput{
		CallbackURL:  "https://httpbin.org/post",
		Payload:      fmt.Sprintf("hello from workflow %s", callbackID),
		DelaySeconds: 5,
	}

	fmt.Printf("Starting delayed callback workflow: %s\n", callbackID)
	fmt.Printf("Will POST to %s after %ds durable sleep\n", callbackInput.CallbackURL, callbackInput.DelaySeconds)

	cbRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        callbackID,
		TaskQueue: TaskQueue,
	}, DelayedCallbackWorkflow, callbackInput)
	if err != nil {
		log.Fatalln("Start callback workflow failed:", err)
	}

	var cbResult string
	if err := cbRun.Get(ctx, &cbResult); err != nil {
		log.Fatalln("Callback workflow failed:", err)
	}
	fmt.Printf("Callback result: %s\n", cbResult)
	fmt.Printf("Search '%s' in the Temporal UI to inspect the history.\n", callbackID)
}
