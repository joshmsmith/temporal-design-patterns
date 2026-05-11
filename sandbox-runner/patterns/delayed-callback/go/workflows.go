package main

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

func OrderWorkflow(ctx workflow.Context, order OrderInput) (string, error) {
	workflow.GetLogger(ctx).Info("Order waiting for payment webhook", "order_id", order.OrderID)

	var payment *PaymentPayload

	// Block until the inbound webhook signal arrives (or timeout after 24 hours)
	selector := workflow.NewSelector(ctx)
	timerFired := false

	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	timer := workflow.NewTimer(timerCtx, 24*time.Hour)

	signalCh := workflow.GetSignalChannel(ctx, SignalName)

	selector.AddReceive(signalCh, func(ch workflow.ReceiveChannel, more bool) {
		ch.Receive(ctx, &payment)
		cancelTimer()
	})
	selector.AddFuture(timer, func(f workflow.Future) {
		if err := f.Get(ctx, nil); err == nil {
			timerFired = true
		}
	})

	selector.Select(ctx)

	if timerFired || payment == nil {
		return "Order " + order.OrderID + ": timed out waiting for payment", nil
	}

	workflow.GetLogger(ctx).Info("Payment signal received", "payment_id", payment.PaymentID)

	ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	var result string
	err := workflow.ExecuteActivity(ao, ProcessPayment, payment).Get(ao, &result)
	return result, err
}

func DelayedCallbackWorkflow(ctx workflow.Context, input CallbackInput) (string, error) {
	workflow.GetLogger(ctx).Info("Sleeping before callback",
		"delay_seconds", input.DelaySeconds, "url", input.CallbackURL)

	// Durable sleep — survives worker restarts, server restarts, everything
	if err := workflow.Sleep(ctx, time.Duration(input.DelaySeconds)*time.Second); err != nil {
		return "", err
	}

	// Fire the outbound callback; Temporal retries on HTTP failure
	ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})
	var result string
	err := workflow.ExecuteActivity(ao, SendWebhookCallback, input).Get(ao, &result)
	return result, err
}
