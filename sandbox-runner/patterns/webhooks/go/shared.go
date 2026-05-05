package main

const (
	TaskQueue        = "webhooks-task-queue"
	WorkflowIDPrefix = "order"
	SignalName       = "payment_received"
)

type OrderInput struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

type PaymentPayload struct {
	PaymentID string  `json:"payment_id"`
	Amount    float64 `json:"amount"`
}
