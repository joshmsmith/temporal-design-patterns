# Temporal Design Patterns

> **Warning:** This catalog is under active development. Content and structure may change.
>
> **Personal project by [@taonic](https://github.com/taonic).**

Temporal provides a set of durable execution primitives that you can compose into common, reusable, and proven patterns.
Having these patterns in your toolbox helps you solve recurring problems in a battle-tested way.

## Distributed transaction patterns {.pattern-section-title}

<div class="pattern-grid">
<div class="pattern-tile">
<a href="saga-pattern">
<div class="pattern-tile-header">
<img src="/images/saga-icon.png" alt="Saga Pattern">
<span>Saga Pattern</span>
</div>
<p>Manages distributed transactions with compensating actions. Each step has a compensation that undoes its effects if subsequent steps fail.</p>
</a>
</div>

<div class="pattern-tile">
<a href="early-return">
<div class="pattern-tile-header">
<img src="/images/early-return-icon.png" alt="Early Return">
<span>Early Return</span>
</div>
<p>Synchronous initialization with asynchronous completion. Returns results immediately while processing continues in the background.</p>
</a>
</div>

<div class="pattern-tile">
<a href="idempotent-distributed-transactions">
<div class="pattern-tile-header">
<img src="/images/saga-icon.png" alt="Idempotent Distributed Transactions">
<span>Idempotent Distributed Transactions</span>
</div>
<p>Coordinates multi-step operations across external services with safe retries, automatic rollback on failure, and protection against duplicate submissions.</p>
</a>
</div>

<div class="pattern-tile">
<a href="distributed-transaction-patterns">
<div class="pattern-tile-header">
<img src="/images/saga-icon.png" alt="Distributed Transaction Patterns Overview">
<span>Distributed Transaction Patterns Overview</span>
</div>
<p>Pattern selection guide for distributed transactions, with a decision tree for choosing between Saga, Early Return, and Idempotent Transactions.</p>
</a>
</div>
</div>

## Entity & lifecycle patterns {.pattern-section-title}

<div class="pattern-grid">
<div class="pattern-tile">
<a href="entity-workflow">
<div class="pattern-tile-header">
<img src="/images/entity-workflow-icon.png" alt="Entity Workflow">
<span>Entity Workflow</span>
</div>
<p>Models long-lived business entities as individual Workflows that persist for the entity's entire lifetime, handling all state transitions through Signals and Updates.</p>
</a>
</div>

<div class="pattern-tile">
<a href="continue-as-new">
<div class="pattern-tile-header">
<img src="/images/continue-as-new-icon.png" alt="Continue-As-New">
<span>Continue-As-New</span>
</div>
<p>Prevents unbounded history growth by completing the current execution and starting a new one with fresh history.</p>
</a>
</div>

<div class="pattern-tile">
<a href="updatable-timer">
<div class="pattern-tile-header">
<img src="/images/updatable-timer-icon.png" alt="Updatable Timer">
<span>Updatable Timer</span>
</div>
<p>Dynamically adjustable timers that respond to Signals or Updates. Extend, shorten, or cancel timers based on external events.</p>
</a>
</div>

<div class="pattern-tile">
<a href="entity-lifecycle-patterns">
<div class="pattern-tile-header">
<img src="/images/entity-workflow-icon.png" alt="Entity & Lifecycle Patterns Overview">
<span>Entity & Lifecycle Patterns Overview</span>
</div>
<p>Pattern selection guide for modeling long-lived stateful entities and managing Workflow history growth over time.</p>
</a>
</div>
</div>

## Workflow messaging patterns {.pattern-section-title}

<div class="pattern-grid">
<div class="pattern-tile">
<a href="signal-with-start">
<div class="pattern-tile-header">
<img src="/images/signal-with-start-icon.png" alt="Signal with Start">
<span>Signal with Start</span>
</div>
<p>Starts a Workflow when Signaling it if it does not already exist. If already running, it receives the Signal directly.</p>
</a>
</div>

<div class="pattern-tile">
<a href="request-response-via-updates">
<div class="pattern-tile-header">
<img src="/images/request-response-icon.png" alt="Request-Response via Updates">
<span>Request-Response via Updates</span>
</div>
<p>Synchronous request-response with validation. Updates modify state and return results directly.</p>
</a>
</div>

<div class="pattern-tile">
<a href="workflow-messaging-patterns">
<div class="pattern-tile-header">
<img src="/images/signal-with-start-icon.png" alt="Workflow Messaging Patterns Overview">
<span>Workflow Messaging Patterns Overview</span>
</div>
<p>Pattern selection guide for sending data into running Workflows and receiving responses or triggering behavior changes.</p>
</a>
</div>
</div>

## Task orchestration patterns {.pattern-section-title}

<div class="pattern-grid">
<div class="pattern-tile">
<a href="child-workflows">
<div class="pattern-tile-header">
<img src="/images/child-workflows-icon.png" alt="Child Workflows">
<span>Child Workflows</span>
</div>
<p>Decomposes complex Workflows into smaller, reusable units. Each child has an independent Workflow ID, history, and lifecycle.</p>
</a>
</div>

<div class="pattern-tile">
<a href="parallel-execution">
<div class="pattern-tile-header">
<img src="/images/parallel-execution-icon.png" alt="Parallel Execution">
<span>Parallel Execution</span>
</div>
<p>Executes multiple Activities concurrently for maximum throughput with error handling and controlled parallelism.</p>
</a>
</div>

<div class="pattern-tile">
<a href="pick-first">
<div class="pattern-tile-header">
<img src="/images/pick-first-icon.png" alt="Pick First">
<span>Pick First (Race)</span>
</div>
<p>Starts multiple Activities in parallel and uses the first result, cancelling the rest.</p>
</a>
</div>

<div class="pattern-tile">
<a href="task-orchestration-patterns">
<div class="pattern-tile-header">
<img src="/images/child-workflows-icon.png" alt="Task Orchestration Patterns Overview">
<span>Task Orchestration Patterns Overview</span>
</div>
<p>Pattern selection guide for composing and coordinating multiple units of work within a Workflow.</p>
</a>
</div>
</div>

## External interaction patterns {.pattern-section-title}

<div class="pattern-grid">
<div class="pattern-tile">
<a href="polling">
<div class="pattern-tile-header">
<img src="/images/polling-icon.png" alt="Polling">
<span>Polling External Services</span>
</div>
<p>Strategies for polling external resources with varying frequencies: frequent, infrequent, and periodic patterns.</p>
</a>
</div>

<div class="pattern-tile">
<a href="long-running-activity">
<div class="pattern-tile-header">
<img src="/images/long-running-activity-icon.png" alt="Long-Running Activity">
<span>Long-Running Activity</span>
</div>
<p>Long-running Activities report progress via heartbeats and enable resumption after failures with cancellation support.</p>
</a>
</div>

<div class="pattern-tile">
<a href="approval">
<div class="pattern-tile-header">
<img src="/images/approval-icon.png" alt="Approval">
<span>Approval</span>
</div>
<p>Human-in-the-loop Workflows that block until external approval decisions are made. Uses Signals to capture approval data with metadata.</p>
</a>
</div>

<div class="pattern-tile">
<a href="delayed-start">
<div class="pattern-tile-header">
<img src="/images/delayed-start-icon.png" alt="Delayed Start">
<span>Delayed Start</span>
</div>
<p>Creates Workflows immediately but defers execution until a specified delay expires. Fits one-time scheduled operations and grace periods.</p>
</a>
</div>

<div class="pattern-tile">
<a href="external-interaction-patterns">
<div class="pattern-tile-header">
<img src="/images/polling-icon.png" alt="External Interaction Patterns Overview">
<span>External Interaction Patterns Overview</span>
</div>
<p>Pattern selection guide for waiting on or interacting with systems and actors outside the Workflow.</p>
</a>
</div>
</div>

## Worker configuration patterns {.pattern-section-title}

<div class="pattern-grid">
<div class="pattern-tile">
<a href="worker-specific-taskqueue">
<div class="pattern-tile-header">
<img src="/images/worker-specific-taskqueue-icon.png" alt="Worker-Specific Task Queues">
<span>Worker-Specific Task Queues</span>
</div>
<p>Routes Activities to specific Workers using unique Task Queues for Worker affinity and host-specific processing.</p>
</a>
</div>

<div class="pattern-tile">
<a href="activity-dependency-injection">
<div class="pattern-tile-header">
<img src="/images/activity-dependency-injection-icon.png" alt="Activity Dependency Injection">
<span>Activity Dependency Injection</span>
</div>
<p>Injects external dependencies into Activities at Worker startup, keeping Workflow code deterministic and Activities testable.</p>
</a>
</div>

<div class="pattern-tile">
<a href="worker-configuration-patterns">
<div class="pattern-tile-header">
<img src="/images/worker-specific-taskqueue-icon.png" alt="Worker Configuration Patterns Overview">
<span>Worker Configuration Patterns Overview</span>
</div>
<p>Pattern selection guide for configuring how Workers are set up, how work is routed, and how Activities access external dependencies.</p>
</a>
</div>
</div>

## Error handling & retry patterns {.pattern-section-title}

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
<p>Override the next retry interval for a specific failure using nextRetryDelay on ApplicationFailure. Use when an error carries information about how long to wait before retrying.</p>
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
<p>Park the Workflow after retries are exhausted and wait for a human to signal a correction, then resume execution from where it left off.</p>
</a>
</div>

<div class="pattern-tile">
<a href="error-handling-patterns">
<div class="pattern-tile-header">
<img src="/images/pick-first-icon.png" alt="Error Handling & Retry Patterns Overview">
<span>Error Handling & Retry Patterns Overview</span>
</div>
<p>Pattern selection guide and decision tree for choosing the right retry strategy based on your error type, cost constraints, and recovery requirements.</p>
</a>
</div>
</div>

## QoS & throughput patterns {.pattern-section-title}

<div class="pattern-grid">
<div class="pattern-tile">
<a href="downstream-rate-limiting">
<div class="pattern-tile-header">
<img src="/images/downstream-rate-limiting-icon.svg" alt="Downstream Rate Limiting">
<span>Downstream Rate Limiting</span>
</div>
<p>Caps Activity execution rate against a downstream service by routing throttled Activities to a dedicated Task Queue backed by Workers configured with a throughput limit.</p>
</a>
</div>

<div class="pattern-tile">
<a href="priority-task-queues">
<div class="pattern-tile-header">
<img src="/images/priority-task-queues-icon.svg" alt="Priority Task Queues">
<span>Priority Task Queues</span>
</div>
<p>Assigns a priority level to Workflows and Activities so that time-sensitive work executes ahead of lower-priority work within a single Task Queue.</p>
</a>
</div>

<div class="pattern-tile">
<a href="fairness">
<div class="pattern-tile-header">
<img src="/images/fairness-icon.svg" alt="Fairness">
<span>Fairness</span>
</div>
<p>Distributes Worker capacity evenly across tenants or users so that a burst from one caller does not starve the others.</p>
</a>
</div>

<div class="pattern-tile">
<a href="qos-throughput-patterns">
<div class="pattern-tile-header">
<img src="/images/downstream-rate-limiting-icon.svg" alt="QoS & Throughput Patterns Overview">
<span>QoS & Throughput Patterns Overview</span>
</div>
<p>Pattern selection guide for controlling execution rate, protecting downstream services from overload, and ensuring fair capacity distribution across tenants.</p>
</a>
</div>
</div>
