
# Error Handling & Retry Patterns

Patterns for controlling how Temporal retries Activities, surfaces persistent failures, and recovers from errors that require human intervention.

<div class="pattern-grid">
<div class="pattern-tile">
<a href="fixed-count-retries">
<div class="pattern-tile-header">
<img src="/images/pick-first-icon.png" alt="Fixed Count of Retries">
<span>Fixed Count of Retries</span>
</div>
<p>Cap the number of Activity retry attempts to control cost when each attempt consumes a paid or limited resource.</p>
</a>
</div>

<div class="pattern-tile">
<a href="fixed-wall-time-retries">
<div class="pattern-tile-header">
<img src="/images/delayed-start-icon.png" alt="Fixed Wall-Time Retries">
<span>Fixed Wall-Time Retries</span>
</div>
<p>Bound the total elapsed time across all retry attempts to enforce a business SLA, regardless of how many individual attempts occur.</p>
</a>
</div>

<div class="pattern-tile">
<a href="non-retryable-errors">
<div class="pattern-tile-header">
<img src="/images/saga-icon.png" alt="Non-Retryable Errors">
<span>Non-Retryable Errors</span>
</div>
<p>Mark error types that will never succeed — such as validation failures or missing records — so Temporal fails fast instead of retrying indefinitely.</p>
</a>
</div>

<div class="pattern-tile">
<a href="delayed-retry">
<div class="pattern-tile-header">
<img src="/images/delayed-start-icon.png" alt="Delayed Retry">
<span>Delayed Retry</span>
</div>
<p>Use a fixed retry interval when you know the downstream system will be unavailable for a predictable duration, such as a scheduled maintenance window.</p>
</a>
</div>

<div class="pattern-tile">
<a href="fast-slow-retries">
<div class="pattern-tile-header">
<img src="/images/polling-icon.png" alt="Fast/Slow Retries">
<span>Fast/Slow Retries</span>
</div>
<p>Try aggressively with a short interval first, then shift to a long interval when fast retries are exhausted, keeping the Workflow alive until the downstream system recovers.</p>
</a>
</div>

<div class="pattern-tile">
<a href="retry-metrics">
<div class="pattern-tile-header">
<img src="/images/downstream-rate-limiting-icon.svg" alt="Retry Alerting via Metrics">
<span>Retry Alerting via Metrics</span>
</div>
<p>Emit a custom metric from inside the Activity when the attempt count crosses a threshold, surfacing silent persistent failures to on-call teams before an SLA breach.</p>
</a>
</div>

<div class="pattern-tile">
<a href="resumable-activity">
<div class="pattern-tile-header">
<img src="/images/approval-icon.png" alt="Resumable Activity">
<span>Resumable Activity</span>
</div>
<p>Park the Workflow after retries are exhausted and wait for a human to signal it's ok to proceed or a data correction, then resume execution from where it left off.</p>
</a>
</div>
</div>

## Choosing the right pattern

The following decision tree helps you select the appropriate retry strategy for your use case.

```mermaid
flowchart TD
    Start([Activity failing]) --> Q1{Each attempt\ncosts money or\nconsumes quota?}
    Q1 -->|Yes| FixedCount[Fixed Count of Retries\nCap MaximumAttempts]
    Q1 -->|No| Q2{Will this error\never succeed\nautomatically?}
    Q2 -->|No| Q2a{Can a human\ncorrect and retry?}
    Q2a -->|Yes| Resumable[Resumable Activity\nPark and await signal]
    Q2a -->|No| NonRetryable[Non-Retryable Errors\nFail fast]
    Q2 -->|Yes| Q3{Downstream has\na predictable\nunavailability window?}
    Q3 -->|Yes| Delayed[Delayed Retry\nFixed interval backoff]
    Q3 -->|No| Q4{Must resolve\nwithin a\ntime budget?}
    Q4 -->|Yes| WallTime[Fixed Wall-Time Retries\nScheduleToCloseTimeout]
    Q4 -->|No| Q5{Want aggressive\ninitial retries then\npatient recovery?}
    Q5 -->|Yes| FastSlow[Fast/Slow Retries\nTwo-phase retry policy]
    Q5 -->|No| Metrics[Retry Alerting via Metrics\nEmit metrics at attempt threshold]
```

The following describes each decision point:

1. If each attempt consumes a paid API call, a rate-limited token, or another scarce resource, use **Fixed Count of Retries** to cap total consumption.
2. If the error is structural — a missing record, invalid input, or authorization failure — and cannot be corrected automatically, ask whether a human can fix it: if so, use **Resumable Activity** to park the Workflow and await a correction signal; otherwise use **Non-Retryable Errors** to fail fast.
3. If the downstream system has a scheduled maintenance window and you know approximately how long it will be unavailable, use **Delayed Retry** with a fixed interval.
4. If the process must resolve (one way or another) within a business SLA window such as 24 hours, use **Fixed Wall-Time Retries** with `ScheduleToCloseTimeout`.
5. If you want to recover from transient errors quickly but also wait indefinitely for the downstream system to come back, use **Fast/Slow Retries**.
6. For any long-running retry scenario, add **Retry Alerting via Metrics** to surface persistent failures before they breach an SLA.

## How Temporal retries work

Temporal's default `RetryPolicy` retries Activities indefinitely with exponential backoff.
Unless you configure a policy, a failing Activity will keep retrying until the `ScheduleToCloseTimeout` or the Workflow itself completes.

The key `RetryPolicy` fields are:

| Field | Default | Effect |
| :--- | :--- | :--- |
| `MaximumAttempts` | 0 (unlimited) | Caps total attempts including the first |
| `InitialInterval` | 1 second | Delay before the first retry |
| `BackoffCoefficient` | 2.0 | Multiplier applied after each retry |
| `MaximumInterval` | 100× InitialInterval | Upper bound on the backoff delay |
| `NonRetryableErrorTypes` | `[]` | Error types that skip retries entirely |

`ScheduleToCloseTimeout` is set on the Activity call options, not in `RetryPolicy`.
It caps the total wall-clock time from when the Activity is first scheduled to when it must complete — across all retry attempts.

## Related patterns

- [Long Running Activity](long-running-activity.md): Heartbeating and resumable progress for Activities that run for minutes to hours.
- [Polling External Services](polling.md): Periodic status checks when the downstream system is asynchronous.
- [Approval](approval.md): Human-in-the-loop gate before a Workflow proceeds.

## References

- [Temporal Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)
- [Understanding Workflow Retries and Failures](https://community.temporal.io/t/understanding-workflow-retries-and-failures/122)
- [Failure Handling in Practice](https://temporal.io/blog/failure-handling-in-practice)
