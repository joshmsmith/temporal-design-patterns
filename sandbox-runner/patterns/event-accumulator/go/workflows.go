package main
package main

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// AccumulatorWorkflow collects order items delivered via signals, deduplicates
// them, and processes them together after a sliding inactivity timeout.
//
// The workflow ID must be derived deterministically from bucketKey so that
// Signal-With-Start routes all signals for the same order to the same instance.
func AccumulatorWorkflow(ctx workflow.Context, bucketKey string, items []OrderItem, seenKeys []string) (string, error) {
	logger := workflow.GetLogger(ctx)

	// Restore deduplication state carried forward from a previous ContinueAsNew run
	seenSet := make(map[string]bool)
	for _, k := range seenKeys {
		seenSet[k] = true
	}
	accumulated := append([]OrderItem{}, items...)

	addItemCh := workflow.GetSignalChannel(ctx, "add-item")
	exitCh := workflow.GetSignalChannel(ctx, "exit")
	exitRequested := false

	for {
		// Drain any signals that arrived before or during the previous iteration
		for {
			var item OrderItem
			if !addItemCh.ReceiveAsync(&item) {
				break
			}
			if item.OrderID == bucketKey && !seenSet[item.ItemID] {
				seenSet[item.ItemID] = true
				accumulated = append(accumulated, item)
			}
		}
		var voidExit interface{}
		if exitCh.ReceiveAsync(&voidExit) {
			exitRequested = true
		}
		if exitRequested {
			break
		}

		// Check history size before entering the next wait
		if workflow.GetInfo(ctx).ContinueAsNewSuggested {
			keys := make([]string, 0, len(seenSet))
			for k := range seenSet {
				keys = append(keys, k)
			}
			logger.Info("Continuing as new", "bucketKey", bucketKey, "count", len(accumulated))
			return "", workflow.NewContinueAsNewError(ctx, AccumulatorWorkflow, bucketKey, accumulated, keys)
		}

		// Sliding window: wait for the next signal or let the inactivity timer fire
		timedOut := false
		timerCtx, cancelTimer := workflow.WithCancel(ctx)
		timer := workflow.NewTimer(timerCtx, maxAwaitTime)
		selector := workflow.NewSelector(ctx)
		selector.AddFuture(timer, func(f workflow.Future) { timedOut = true })
		selector.AddReceive(addItemCh, func(c workflow.ReceiveChannel, _ bool) {
			var item OrderItem
			c.Receive(ctx, &item)
			if item.OrderID == bucketKey && !seenSet[item.ItemID] {
				seenSet[item.ItemID] = true
				accumulated = append(accumulated, item)
			}
		})
		selector.AddReceive(exitCh, func(c workflow.ReceiveChannel, _ bool) {
			var void interface{}
			c.Receive(ctx, &void)
			exitRequested = true
		})
		selector.Select(ctx)
		cancelTimer() // no-op if timer already fired; cancels timer if a signal arrived first

		if timedOut || exitRequested {
			break
		}
	}

	// Inactivity timeout or exit signal — process the accumulated batch
	ao := workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second}
	actCtx := workflow.WithActivityOptions(ctx, ao)
	var result string
	if err := workflow.ExecuteActivity(actCtx, ProcessItems, bucketKey, accumulated).Get(ctx, &result); err != nil {
		return "", err
	}
	logger.Info("Processed order batch", "bucketKey", bucketKey, "count", len(accumulated))
	return result, nil
}
