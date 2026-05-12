
<h1>Delayed Retry <img src="/images/delayed-start-icon.png" alt="Delayed Retry" class="pattern-page-icon"></h1>

:::info TLDR
Throw an `ApplicationFailure` with `nextRetryDelay` set inside the Activity to **delay the next retry for a fixed time.** Use this when an error carries its own timing information — such as an HTTP 429 `Retry-After` header or a known maintenance window — so Temporal waits exactly as long as needed instead of following the generic backoff schedule.
:::

## Overview

The Delayed Retry pattern overrides the next retry interval for a specific failure by throwing an `ApplicationFailure` with a `nextRetryDelay` field set from inside the Activity.
Use it when a particular error carries information about how long to wait before retrying — such as a rate-limit response with a `Retry-After` header, or a known maintenance window with a fixed end time.

> **SDK availability:** `nextRetryDelay` is supported in the **Java**, **TypeScript**, and **Rust** SDKs. 

## Problem

A `RetryPolicy` applies a single backoff schedule to all failures from an Activity.
This works well for generic transient errors, but some errors carry specific information about how long the caller must wait:

- An HTTP 429 response includes a `Retry-After: 60` header telling you exactly when the quota resets.
- A downstream system returns an error message saying "maintenance until 02:00 UTC" — a precise, known delay.
- A database error includes a lock timeout duration that indicates when the resource will be available.

With a global `RetryPolicy`, you have two bad options: set a short interval and retry too early (wasting quota and adding load), or set a long interval and wait longer than necessary.
What you need is to set the next retry delay *per failure*, based on the information the error itself provides.

## Solution

Throw an `ApplicationFailure` with the `nextRetryDelay` field set from inside the Activity.
Temporal replaces the RetryPolicy-calculated interval for that single retry with the value you specify.
Subsequent retries (if the next attempt also fails) return to the normal RetryPolicy schedule unless you set `nextRetryDelay` again.

```mermaid
sequenceDiagram
    participant Workflow
    participant Temporal as Temporal Service
    participant API as Rate-Limited API

    Workflow->>Temporal: Schedule activity (RetryPolicy: initialInterval=1s)
    Temporal->>+API: Attempt 1
    API-->>-Temporal: HTTP 429 — Retry-After: 60s
    Note over Temporal: Activity throws ApplicationFailure(nextRetryDelay=60s)
    Note over Temporal: Override: wait 60s (ignoring RetryPolicy interval)
    Temporal->>+API: Attempt 2
    API-->>-Temporal: Success
    Temporal-->>Workflow: Result
```

The following describes each step:

1. The Activity calls the API. It receives an HTTP 429 with a `Retry-After: 60` header.
2. The Activity extracts the retry delay from the response and throws `ApplicationFailure` with `nextRetryDelay=60s`.
3. Temporal ignores the RetryPolicy's calculated interval for this retry and waits exactly 60 seconds instead.
4. The next attempt succeeds and Temporal delivers the result to the Workflow.

## Implementation

<DaytonaRunner pattern="delayed-retry" />

### Overriding the retry delay from the response

Extract the wait duration from the error or response and pass it to `ApplicationFailure`.
The RetryPolicy's `MaximumAttempts` and `ScheduleToCloseTimeout` still apply — only the interval for the next retry is overridden.

::: code-group
```java [Java]
// RateLimitedActivityImpl.java
import io.temporal.activity.Activity;
import io.temporal.failure.ApplicationFailure;
import java.time.Duration;

public class RateLimitedActivityImpl implements RateLimitedActivity {
    @Override
    public String callApi(String endpoint) {
        ApiResponse response = httpClient.get(endpoint);

        if (response.getStatusCode() == 429) {
            int retryAfterSeconds = response.getHeaderInt("Retry-After", 60);
            throw ApplicationFailure.newFailureWithCauseAndDelay(
                "Rate limited — retrying after " + retryAfterSeconds + "s",
                "RateLimitError",
                null,
                Duration.ofSeconds(retryAfterSeconds)
            );
        }

        return response.getBody();
    }
}
```

```typescript [TypeScript]
// activities.ts
import { ApplicationFailure } from '@temporalio/activity';

export async function callApi(endpoint: string): Promise<string> {
    const response = await fetch(endpoint);

    if (response.status === 429) {
        const retryAfter = parseInt(response.headers.get('Retry-After') ?? '60', 10);
        throw ApplicationFailure.create({
            message: `Rate limited — retrying after ${retryAfter}s`,
            type: 'RateLimitError',
            nextRetryDelay: `${retryAfter}s`,
        });
    }

    return response.text();
}
```
:::

### Attempt-proportional delay

You can also set the delay dynamically based on the attempt number — for example, to implement a custom backoff that differs from exponential, or to add a known base delay on top of the standard backoff.

::: code-group
```java [Java]
// BackoffActivityImpl.java
import io.temporal.activity.Activity;
import io.temporal.failure.ApplicationFailure;
import java.time.Duration;

public class BackoffActivityImpl implements BackoffActivity {
    @Override
    public String process(String input) {
        int attempt = Activity.getExecutionContext().getInfo().getAttempt();

        try {
            return downstreamService.call(input);
        } catch (ServiceUnavailableException e) {
            // Custom delay: 3 seconds × attempt number (3s, 6s, 9s, …)
            throw ApplicationFailure.newFailureWithCauseAndDelay(
                "Service unavailable on attempt " + attempt,
                "ServiceUnavailable",
                e,
                Duration.ofSeconds(3L * attempt)
            );
        }
    }
}
```

```typescript [TypeScript]
// activities.ts
import { ApplicationFailure, activityInfo } from '@temporalio/activity';

export async function process(input: string): Promise<string> {
    const { attempt } = activityInfo();

    try {
        return await downstreamService.call(input);
    } catch (e) {
        // Custom delay: 3 seconds × attempt number (3s, 6s, 9s, …)
        throw ApplicationFailure.create({
            message: `Service unavailable on attempt ${attempt}`,
            type: 'ServiceUnavailable',
            cause: e as Error,
            nextRetryDelay: `${3 * attempt}s`,
        });
    }
}
```
:::

### Workflow configuration

The Workflow sets a normal `RetryPolicy`.
The `nextRetryDelay` set in the Activity overrides the interval only for the retry following that specific failure — subsequent attempts fall back to the RetryPolicy schedule if `nextRetryDelay` is not set again.

::: code-group
```java [Java]
// ApiWorkflowImpl.java
public class ApiWorkflowImpl implements ApiWorkflow {
    private final RateLimitedActivity activities = Workflow.newActivityStub(
        RateLimitedActivity.class,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(10))
            .setRetryOptions(RetryOptions.newBuilder()
                .setInitialInterval(Duration.ofSeconds(1))
                .setBackoffCoefficient(2.0)
                .setMaximumAttempts(10)
                .build())
            .build()
    );

    @Override
    public String run(String endpoint) {
        return activities.callApi(endpoint);
    }
}
```

```typescript [TypeScript]
// workflows.ts
import * as wf from '@temporalio/workflow';
import type * as activities from './activities';

const { callApi } = wf.proxyActivities<typeof activities>({
    startToCloseTimeout: '10s',
    retry: {
        initialInterval: '1s',
        backoffCoefficient: 2,
        maximumAttempts: 10,
    },
});

export async function apiWorkflow(endpoint: string): Promise<string> {
    return await callApi(endpoint);
}
```
:::

## Best practices

- **Use the error's own delay information when available.** HTTP 429 `Retry-After`, database lock timeouts, and API-provided backoff hints are more accurate than any value you could configure statically.
- **Fall back to the RetryPolicy for unknown errors.** Only set `nextRetryDelay` for error types where you have reliable delay information. Let the RetryPolicy handle all other failures normally.
- **Still set a meaningful RetryPolicy.** `nextRetryDelay` overrides a single retry interval; the RetryPolicy governs everything else — maximum attempts, schedule-to-close timeout, and fallback intervals.
- **Log the override.** When setting a non-standard delay, log the delay value and its source (e.g., the `Retry-After` header value) so the Workflow history and your logging backend show why the Activity waited an unusual amount of time.
- **Set `startToCloseTimeout` to be more than the downstream call typically takes.** You want the downstream call to fail before the Activity times out. For example, for an HTTP request that times out after 30s, have the `startToCloseTimeout` be 45 or 60 seconds.

## Common pitfalls

- **Assuming `nextRetryDelay` persists across all retries.** It only applies to the immediate next retry. If the following attempt also fails without setting `nextRetryDelay`, the RetryPolicy interval resumes.
- **Setting `nextRetryDelay` longer than `ScheduleToCloseTimeout`.** If the override delay exceeds the remaining `ScheduleToCloseTimeout` budget, the retry will never execute — Temporal will expire the Activity before the delay elapses.
- **Using `nextRetryDelay` in Go or Python.** This feature is not available in those SDKs. For Go and Python, use `BackoffCoefficient=1.0` with a fixed `InitialInterval` to approximate a fixed delay.
- **Setting `startToCloseTimeout` to be less than the downstream call typically takes.** If you set the `startToCloseTimeout` to be less than your call timeout, the Activity will be failed (and usually retried) even though the downstream call may have succeeded at the last moment.

## Related patterns

- [Fixed Count of Retries](fixed-count-retries.md): Cap total attempts to prevent unbounded retry cost.
- [Fixed Wall-Time Retries](fixed-wall-time-retries.md): Enforce a total elapsed time budget across all attempts.
- [Fast/Slow Retries](fast-slow-retries.md): Shift from a fast retry cadence to a slow one after initial attempts are exhausted.
- [Error Handling & Retry Patterns](error-handling-patterns.md): Overview and decision tree for all retry patterns.

## References

- [Per-error next Retry delay](https://docs.temporal.io/encyclopedia/retry-policies#per-error-next-retry-delay)
- [Customize retry delays per error in the Java SDK](https://docs.temporal.io/develop/java/activities/timeouts#activity-next-retry-delay)
- [Customize retry delays per error in the TypeScript SDK](https://docs.temporal.io/develop/typescript/activities/timeouts#activity-next-retry-delay)
- [Customize retry delays per error in the Rust SDK](https://docs.temporal.io/develop/rust/activities/timeouts#next-retry-delay)

