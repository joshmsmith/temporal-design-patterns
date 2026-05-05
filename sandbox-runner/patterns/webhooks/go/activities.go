package main

import (
	"context"
	"fmt"
)

func ProcessPayment(_ context.Context, payload *PaymentPayload) (string, error) {
	fmt.Printf("Processing payment %s for $%.2f\n", payload.PaymentID, payload.Amount)
	return fmt.Sprintf("Payment %s processed successfully for $%.2f", payload.PaymentID, payload.Amount), nil
}
