
<h1>Non-Retryable Errors <img src="/images/saga-icon.png" alt="Non-Retryable Errors" class="pattern-page-icon"></h1>

## Overview

The Non-Retryable Errors pattern marks specific error types so Temporal stops retrying immediately when one is raised.
Use it for failures where the root cause is structural — invalid input, a missing record, an authorization problem — where repeating the same call will never produce a different result.

## Problem

Temporal retries all Activity failures by default.
For transient infrastructure errors such as network timeouts or service restarts, this is the right behavior.
But some failures are permanent: no amount of retrying will fix them.

Retrying a permanent failure wastes time and resources:

- A transfer to a non-existent account number will fail on attempt 1, 2, and 3 in exactly the same way.
- An API call with a malformed request body will be rejected every time.
- A request from a revoked API key will receive an authorization error on every attempt.

With the default unlimited retry policy, the Workflow waits through exponential backoff delays — minutes to hours — before eventually delivering the error to the Workflow, when it could have failed in milliseconds.

## Solution

Raise a non-retryable error from inside the Activity when the failure is known to be permanent.
Temporal detects the error type and skips all remaining retries, delivering the failure to the Workflow immediately.

There are two complementary mechanisms:

1. **Mark the error as non-retryable at the throw site** — the Activity explicitly signals that this specific failure should not be retried.
2. **Register non-retryable error types in the `RetryPolicy`** — the Workflow declares which error type names should never be retried, regardless of how the Activity raises them.

Both mechanisms can be used together.

```mermaid
flowchart TD
    Workflow -->|Schedule Activity| Activity
    Activity -->|Raises error| Check{Is error type\nnon-retryable?}
    Check -->|Yes — marked at throw site\nor listed in RetryPolicy| Fail([Deliver ActivityError\nto Workflow immediately])
    Check -->|No| Retry[Schedule retry\nwith backoff]
    Retry --> Activity
    Fail --> Handle[Workflow handles error:\nlog, compensate, or escalate]
```

The following describes each path:

1. The Activity raises an error. Temporal inspects whether the error type is non-retryable.
2. If non-retryable — either because the Activity flagged it or the `RetryPolicy` lists the type — Temporal delivers the `ActivityError` to the Workflow without delay.
3. If retryable, Temporal schedules another attempt after the configured backoff.
4. The Workflow catches the `ActivityError` and handles it according to the business logic.

## Implementation

### Marking an error as non-retryable at the throw site

Use the SDK's `ApplicationError` (or equivalent) with the non-retryable flag.
Temporal propagates the error type name and the flag to the Workflow without retrying.

::: code-group
```python [Python]
# activities.py
from temporalio import activity
from temporalio.exceptions import ApplicationError

@activity.defn
async def process_order(order_id: str) -> str:
    order = await db.get_order(order_id)
    if order is None:
        raise ApplicationError(
            f"Order {order_id} not found",
            type="OrderNotFoundError",
            non_retryable=True,
        )
    if not order.is_valid():
        raise ApplicationError(
            f"Order {order_id} failed validation: {order.validation_errors}",
            type="ValidationError",
            non_retryable=True,
        )
    return await payment_service.charge(order)
```

```go [Go]
// activities.go
package orders

import (
    "context"
    "fmt"

    "go.temporal.io/sdk/temporal"
)

func ProcessOrder(ctx context.Context, orderID string) (string, error) {
    order, err := db.GetOrder(orderID)
    if err != nil || order == nil {
        return "", temporal.NewNonRetryableApplicationError(
            fmt.Sprintf("order %s not found", orderID),
            "OrderNotFoundError",
            err,
        )
    }
    if !order.IsValid() {
        return "", temporal.NewNonRetryableApplicationError(
            fmt.Sprintf("order %s failed validation: %v", orderID, order.ValidationErrors()),
            "ValidationError",
            nil,
        )
    }
    return paymentService.Charge(order)
}
```

```java [Java]
// ProcessOrderActivityImpl.java
import io.temporal.failure.ApplicationFailure;

public class ProcessOrderActivityImpl implements ProcessOrderActivity {
    @Override
    public String processOrder(String orderId) {
        Order order = db.getOrder(orderId);
        if (order == null) {
            throw ApplicationFailure.newNonRetryableFailure(
                "Order " + orderId + " not found",
                "OrderNotFoundError"
            );
        }
        if (!order.isValid()) {
            throw ApplicationFailure.newNonRetryableFailure(
                "Order " + orderId + " failed validation: " + order.getValidationErrors(),
                "ValidationError"
            );
        }
        return paymentService.charge(order);
    }
}
```

```typescript [TypeScript]
// activities.ts
import { ApplicationFailure } from '@temporalio/activity';

export async function processOrder(orderId: string): Promise<string> {
    const order = await db.getOrder(orderId);
    if (!order) {
        throw ApplicationFailure.nonRetryable(
            `Order ${orderId} not found`,
            'OrderNotFoundError',
        );
    }
    if (!order.isValid()) {
        throw ApplicationFailure.nonRetryable(
            `Order ${orderId} failed validation: ${order.validationErrors}`,
            'ValidationError',
        );
    }
    return paymentService.charge(order);
}
```
:::

### Declaring non-retryable types in the RetryPolicy

Alternatively, list error type names in the `RetryPolicy` at the Workflow call site.
Temporal stops retrying when the Activity raises an error whose type matches any name in the list.
This approach separates the retry decision from the Activity code, which is useful when the Activity is shared and the non-retryable classification depends on the caller's context.

::: code-group
```python [Python]
# workflows.py
from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
import activities

@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order_id: str) -> str:
        return await workflow.execute_activity(
            activities.process_order,
            order_id,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                non_retryable_error_types=["OrderNotFoundError", "ValidationError"],
            ),
        )
```

```go [Go]
// workflow.go
ao := workflow.ActivityOptions{
    StartToCloseTimeout: 10 * time.Second,
    RetryPolicy: &temporal.RetryPolicy{
        NonRetryableErrorTypes: []string{"OrderNotFoundError", "ValidationError"},
    },
}
ctx = workflow.WithActivityOptions(ctx, ao)
```

```java [Java]
// OrderWorkflowImpl.java
private final ProcessOrderActivity activities = Workflow.newActivityStub(
    ProcessOrderActivity.class,
    ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(10))
        .setRetryOptions(RetryOptions.newBuilder()
            .setDoNotRetry("OrderNotFoundError", "ValidationError")
            .build())
        .build()
);
```

```typescript [TypeScript]
// workflows.ts
const { processOrder } = wf.proxyActivities<typeof activities>({
    startToCloseTimeout: '10s',
    retry: {
        nonRetryableErrorTypes: ['OrderNotFoundError', 'ValidationError'],
    },
});
```
:::

### Handling non-retryable errors in the Workflow

Catch the `ActivityError` in the Workflow to distinguish between permanent failures and transient ones.
Inspect the underlying cause to route to the appropriate compensation or escalation path.

::: code-group
```python [Python]
# workflows.py
from temporalio.exceptions import ActivityError, ApplicationError

@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order_id: str) -> str:
        try:
            return await workflow.execute_activity(
                activities.process_order,
                order_id,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(
                    non_retryable_error_types=["OrderNotFoundError", "ValidationError"],
                ),
            )
        except ActivityError as e:
            cause = e.__cause__
            if isinstance(cause, ApplicationError):
                if cause.type == "ValidationError":
                    return f"Order rejected: {cause}"
                if cause.type == "OrderNotFoundError":
                    return f"Order not found: {cause}"
            raise
```

```go [Go]
// workflow.go
var result string
err := workflow.ExecuteActivity(ctx, ProcessOrder, orderID).Get(ctx, &result)
if err != nil {
    var appErr *temporal.ApplicationError
    if errors.As(err, &appErr) {
        switch appErr.Type() {
        case "ValidationError":
            return "", fmt.Errorf("order rejected: %w", appErr)
        case "OrderNotFoundError":
            return "", fmt.Errorf("order not found: %w", appErr)
        }
    }
    return "", err
}
```

```java [Java]
// OrderWorkflowImpl.java
try {
    return activities.processOrder(orderId);
} catch (ActivityFailure e) {
    if (e.getCause() instanceof ApplicationFailure appFailure) {
        switch (appFailure.getType()) {
            case "ValidationError" -> { return "Order rejected: " + appFailure.getMessage(); }
            case "OrderNotFoundError" -> { return "Order not found: " + appFailure.getMessage(); }
        }
    }
    throw e;
}
```

```typescript [TypeScript]
// workflows.ts
try {
    return await processOrder(orderId);
} catch (err) {
    if (err instanceof wf.ActivityFailure && err.cause instanceof wf.ApplicationFailure) {
        if (err.cause.type === 'ValidationError') {
            return `Order rejected: ${err.cause.message}`;
        }
        if (err.cause.type === 'OrderNotFoundError') {
            return `Order not found: ${err.cause.message}`;
        }
    }
    throw err;
}
```
:::

## Best practices

- **Validate input before scheduling the Activity.** If the Workflow can detect invalid input upfront — using an `Update` validator or by inspecting the input data — fail fast in the Workflow rather than paying the cost of an Activity execution.
- **Use specific error type names.** Generic names like `"Error"` or `"Failure"` match too broadly. Use domain-specific names like `"OrderNotFoundError"` or `"InsufficientFundsError"` so the Workflow can distinguish between failure causes.
- **Reserve non-retryable for truly permanent failures.** A rate-limit error (HTTP 429) is transient — the same call will succeed after a delay. A not-found error (HTTP 404) is typically permanent. Match the non-retryable classification to the nature of the error.
- **Combine both mechanisms for defence in depth.** Mark the error as non-retryable at the throw site so the Activity is self-describing, and also list the type in the `RetryPolicy` so the classification is enforced even if the Activity code changes.

## Common pitfalls

- **Marking transient errors as non-retryable.** Network timeouts and service unavailability are transient. Marking them non-retryable removes Temporal's ability to recover automatically.
- **Using the error message instead of a type name.** `RetryPolicy.NonRetryableErrorTypes` matches on type names, not message strings. Without a type name, the policy cannot identify the error.
- **Swallowing the `ActivityError` without logging.** Non-retryable errors fail fast and silently if you do not catch and log them. Always log the failure before re-raising or returning an error result.
- **Confusing non-retryable errors with Workflow failures.** A non-retryable `ActivityError` fails the Activity and delivers the error to the Workflow. The Workflow itself does not fail unless it re-raises the error without catching it.

## Related patterns

- [Fixed Count of Retries](fixed-count-retries.md): Limit retries for transient errors that are worth retrying a bounded number of times.
- [Resumable Activity](resumable-activity.md): Park the Workflow and accept a corrected input via Signal when retries are exhausted.
- [Error Handling & Retry Patterns](error-handling-patterns.md): Overview and decision tree for all retry patterns.

## References

- [Temporal Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)
- [Failure Handling in Practice](https://temporal.io/blog/failure-handling-in-practice)
