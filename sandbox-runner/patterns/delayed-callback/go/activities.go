package main

import (
	"context"
	"fmt"
)

func ProcessPayment(_ context.Context, payload *PaymentPayload) (string, error) {
	fmt.Printf("Processing payment %s for $%.2f\n", payload.PaymentID, payload.Amount)
	return fmt.Sprintf("Payment %s processed successfully for $%.2f", payload.PaymentID, payload.Amount), nil
}

func SendWebhookCallback(_ context.Context, input CallbackInput) (string, error) {
	// In production this would POST to input.CallbackURL with input.Payload as the body.
	// For this demo we skip the actual HTTP call.
	fmt.Printf("[stub] Would POST to %s with payload: %s\n", input.CallbackURL, input.Payload)
	return fmt.Sprintf("Callback to %s delivered (stubbed)", input.CallbackURL), nil
}
