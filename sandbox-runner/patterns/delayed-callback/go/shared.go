package main

const (
	TaskQueue               = "webhooks-task-queue"
	WorkflowIDPrefix        = "order"
	CallbackWorkflowIDPrefix = "delayed-callback"
	SignalName              = "payment_received"
)

type OrderInput struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

type PaymentPayload struct {
	PaymentID string  `json:"payment_id"`
	Amount    float64 `json:"amount"`
}

type CallbackInput struct {
	CallbackURL  string `json:"callback_url"`
	Payload      string `json:"payload"`
	DelaySeconds int    `json:"delay_seconds"`
}
