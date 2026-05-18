# SA Workflow Design & Best Practices Guide

[**Overview	2**](#overview)

[📋  Prerequisites	2](#📋-prerequisites)

[🎯  Goals	2](#🎯-goals)

[⛔  Non-Goals	2](#⛔-non-goals)

[**Preparation	2**](#preparation)

[For the SA	2](#for-the-sa)

[For the Customer	3](#for-the-customer)

[**Workflow Design	4**](#workflow-design)

[General	4](#general)

[Common Design Questions	5](#common-design-questions)

[**Best Practices	8**](#best-practices)

[Workflows	8](#workflows)

[Child Workflows	10](#child-workflows)

[Activities	11](#activities)

[Signals	13](#signals)

[Queries	15](#queries)

[Update	15](#update)

[Workers & Task Queues	16](#workers-&-task-queues)

[Timers	17](#timers)

[Schedules	17](#schedules)

[Cron Jobs	18](#cron-jobs)

[Side Effects	19](#side-effects)

[Data Converter	19](#data-converter)

[Visibility	19](#visibility)

[Versioning	20](#versioning)

[Interceptors	21](#interceptors)

[Sessions/Worker-Specific Task Queues	21](#sessions/worker-specific-task-queues)

[Storage Optimization (Long Running Workflows)	21](#storage-optimization-\(long-running-workflows\))

[**Appendix	21**](#appendix)

[1\. Best Practices Checklist	21](#best-practices-checklist)

[2\. Useful Links: SDK Features Matrix	23](#useful-links:)

# 

# Overview {#overview}

## 📋  Prerequisites {#📋-prerequisites}

The customer has already vetted Temporal and/or Temporal Cloud as a suitable platform for building their use case(s).

## 🎯  Goals {#🎯-goals}

The goals of a Workflow design session are to:

* Ensure the customer is using the Temporal primitives correctly and aligned with our best practices  
* Answer specific customer questions on Workflow design  
* Advise on any gotchas or potential issues with the Workflow  
* Build customer confidence on their path to production with Temporal

## ⛔  Non-Goals {#⛔-non-goals}

* Discover or Confirm Temporal or Temporal Cloud suitability for this use case. See [SA Customer Evaluation Runbook](https://docs.google.com/document/d/18f5K7wmOKy5luT1gajKqkCuys1q9tNkM9DwDNXVsTmw/edit#heading=h.o0epy3bs2n54)for that.  
* Deep debugging or troubleshooting of production issues (enter a support ticket for that)  
* Perfection: no 1-hour session will yield a perfect workflow. It will be an iterative process

# Preparation {#preparation}

## For the SA {#for-the-sa}

The SA must be familiar with all of the Temporal concepts and primitives in this document.   This document is not a replacement for the [Temporal documentation](https://docs.temporal.io/), the [Developer’s Guide](https://docs.temporal.io/dev-guide), the [SDK Samples](https://github.com/temporalio?q=samples-&type=public&language=&sort=stargazers), the [Community Forums](https://community.temporal.io/), or Slack.  Rather, this document highlights (and references) knowledge from those sources that is relevant to Workflow Design and Best Practices.

This guide should help the SA focus on key Workflow design elements and usage of Temporal primitives in a Workflow.  However, it will take time for you to review and become familiar with all of the topics.  Take that time.  Read the docs.  Play with the samples.  Shadow your fellow SAs.  Contribute back to this doc with your newly acquired knowledge.  Relax with your favorite beverage as you bask in the growth of your Temporal expertise and the value you bring to our customers everyday.  Cheers\!

## For the Customer {#for-the-customer}

Prior to the meeting, request that your customer provides the following:

* A description of the use case and the business process during which it runs  
* A diagram of the Workflow with Activities, etc.  
* Access to Workflow Execution history in the Temporal UI (if code has been written and run successfully)  
* Access to code (if code has been written and customer is comfortable with sharing) 

## Suggested Session Outline

# Workflow Design {#workflow-design}

The following sections include questions, prompts and areas to guide the user throughout the design session.

## General {#general}

### ● What is the customer’s **level of experience** with Temporal?

* Is this the first Workflow they have designed or implemented?  
* What is their overall comfort level with and knowledge of Temporal, its primitives, and the execution model?

### ● Have the customer **walk through the diagram**. 

* What is the business process?  
* Where does Temporal fit in the overall architecture?  
  * Is everything a Workflow, or is it just being used for one part of the overall system?  
* What invokes the Workflow?    
  * Is it a user action, a system event, or a cron/schedule?  
* How often is the Workflow called?    
  * What is the expected peak volume? (e.g. 10x/day? 10x/second? More? Less?)  
* How long does the Workflow run?   
  * Does it complete in seconds? Months? Longer?  
* How many Activities, or other steps/actions, are in the Workflow?    
  * Are they sequential, or do they run in parallel?  
* What is done in the Activities?  
  * Are Activities too granular or too broad?    
  * Are there multiple steps within an Activity?

At this point, the discussion may still be high level. Perhaps your customer has attempted to steer the conversation in another direction.  

* Ensure you have a good high level understanding before going too deep into the weeds.    
* If your customer has immediate questions, and areas to focus on, you can follow their lead using the sections below to guide your review of their Workflows, Activities, Signals, Queries, etc… as needed.  
* If they bring you deep into code, you can pull back out (if you need to) by asking to view the workflow history in the UI, or revisit the diagram

### ● **Gut check** for Use Case and Design

* #### Is this an appropriate use-case for Temporal?

  * Almost anything can be a valid use case, but watch out for:  
    * Extreme low latency, e.g. high-frequency algo trading where milliseconds matter  
    * Synchronous read-only operations, i.e. if you just need to query records out of a DB, you don’t need Temporal.  
    * Big data passing through Temporal.  Temporal is a control-plane, it should not be used for the data-plane.

* #### Indicators of a good design

  * Workflow [determinism](#✔-do-they-understand-workflow-determinism-requirements?)  
  * Activity [idempotency](#✔-do-they-understand-activity-idempotency-as-a-best-practice?)  
  * Appropriate management of the [Event History](#✔-do-they-understand-the-event-history?) size  
  * Understanding and consideration of [Temporal limits](https://docs.temporal.io/self-hosted-guide/defaults) and [Cloud limits](https://docs.temporal.io/cloud/limits)  
  * Appropriate use of [timeouts](#✔-what-are-the-activity-timeout-settings?) and [retry](#✔-what-is-the-activity-retry-policy?) options

* #### Indicators of a sub-optimal design

  * DIY state management  
    * Are they unnecessarily persisting status or entities to a database?  
      * It is valid/necessary to persist data for historical, reporting or aggregate query purposes  
    * Are they sending messages (via Kafka, RabbitMQ, etc.) for the purpose of choreography to other components in the system?  (perhaps those should be Workflows as well)  
  * Misuse of [Child Workflows](#✔-do-they-use-child-workflows?)  
  * Misuse of [Local Activities](#✔-do-they-use-local-activities?)  
  * Misuse of [Search Attributes](#✔-do-they-use-search-attributes?)

## Common Design Questions {#common-design-questions}

### ● When to use Local Activity (vs. Activity)?

* Use regular Activities unless your use case requires very high throughput and large Activity fan-outs of very short-lived Activities.  
* Reference:  
  * Best practices for [Local Activities](#✔-do-they-use-local-activities?)  
  * Community forum: [Local Activity vs Activity](https://community.temporal.io/t/local-activity-vs-activity/290/3)  
  * Slide deck [Workflow Latency: Regular Activities vs Local Activities](https://docs.google.com/presentation/d/1BFipVEynxs5-fC7aeOsPO4xSeWan5L7-0WTdIi1DZU0/edit#slide=id.p)

### ● When to use Child Workflow (vs. Activity)?

* When in doubt, use an Activity  
* Reference:  
  * Best practices for [Child Workflows](#✔-do-they-use-child-workflows?)  
  * Docs: [When to use a Child Workflow versus an Activity](https://docs.temporal.io/encyclopedia/child-workflows#child-workflow-versus-an-activity)

### ● Can a WF communicate with another WF?

* Yes, via Nexus sync operations  
* Yes. Can be done with:  
  * Signals  
  * Queries  
  * Updates

  You can Signal from a Workflow and Query/Update through an Activity within a Workflow.


### ● Can a WF communicate with another WF **in a different Namespace**?

* Yes, via Nexus  
* Old Way – Yes through an Activity as follows:  
  * Create a new Temporal Client  
  * Use that Client within an Activity to Signal, Query or Update the WF in another Namespace  
  * See for reference: [this sample](https://github.com/temporalio/samples-java/tree/main/core/src/main/java/io/temporal/samples/batch/slidingwindow)

### ● How to handle large payloads?

* Pass references to data (e.g. filenames/handles)  
* Consider [External Storage](https://docs.temporal.io/external-storage)  
  * (Previously: Consider [Temporal Large Payload Codec](https://github.com/DataDog/temporal-large-payload-codec) (from DataDog) to automatically replace data with reference  
  * Some caution using a Large Payload Codec  
    * Causes a remote access for every payload  
    * Do it explicitly on a case-by-case basis (Don’t do this implicitly)  
    * Lots of remote writes/reads can affect workflow performance.  
* Consider moving the work into activities   
  * The first activity takes the ID and gets the data and continues the workflow  
  * The last activity takes the results, stores it and returns an ID  
  * Challenge is knowing which bits of data are needed inside the workflow logic  
* Store data on local disk and use sticky Activities  
* Consider a broader Activity that gathers AND processes data within a single Activity  
* Reference:   
  * Best practices for [Activity Input and Outputs](#✔-what-are-the-input-and-output-sizes-for-activity-payloads?)

### ● How to handle large Workflow History?

* Use Continue-As-New at or before the WARN levels (10mb or 10k events)  
  * Note that Cloud users will *not* see the default warning, as it is emitted on the server side, however they can obtain WF history length through the SDK.  
  * Also, the SDKs now contain a “suggestContinueAsNew()” API that can be used to determine when to CAN.  
    * Java \- [Workflow.getInfo().isContinueAsNewSuggested()](https://www.javadoc.io/doc/io.temporal/temporal-sdk/latest/io/temporal/workflow/WorkflowInfo.html)  
    * Go \- [GetContinueAsNewSuggested()](https://pkg.go.dev/go.temporal.io/sdk@v1.25.1/internal#GetWorkflowInfo) from GetWorkflowInfo  
    * Python \- [is\_continue\_as\_new\_suggested](https://python.temporal.io/temporalio.workflow.Info.html#is_continue_as_new_suggested) from Workflow.info  
    * TypeScript \- [workflowInfo().continueAsNewSuggested](https://typescript.temporal.io/api/interfaces/workflow.WorkflowInfo#continueasnewsuggested)  
    * Dotnet \- [Workflow.ContinueAsNewSuggested](https://dotnet.temporal.io/api/Temporalio.Workflows.Workflow.html#Temporalio_Workflows_Workflow_ContinueAsNewSuggested)  
* Use Child Workflows to partition the work  
* Reference:   
  * Best practices for [WF Event History](#✔-do-they-understand-the-event-history?)

### ● How to run Activities / Child WFs in parallel and aggregate results?

* All executions by the API (e.g. Execute Activity, Execute Child WF) return some form of a Promise, Future, or Awaitable (depending on the SDK).  
* This is by default asynchronous invocation.  To run execs in parallel, just don’t block on getting the result of a promise  
* Gather the promises in an array and when appropriate, iterate over the array and Get the results from the promises

### ● When to use Worker-specific Activity Task Queues?

Use Worker-specific Activity Task Queues for:

* Special purpose hardware or capabilities for workers (e.g. GPUs)  
* Activities that need to run on the same worker   
  * Such as when workers need to access local filesystem files.  
  * Such as when workers need to run on a machine with elevated access.  
  * Such as when workers need to run on a machine with differentiated hardware, such as GPUs.  
* Rate-limiting  
* Reference:   
  * Best practices on [Activity specific task queues](#✔-do-any-activities-run-on-unique-task-queues?)

### ● When to use a schedule vs a timer?

* Use timers when:  
  * Your delay is relative (e.g. wait 2 days)  
  * Your delay happens inside a Workflow Execution  
  * Would exceed 200 schedule actions per second (default is 10\)  
* Use schedules when:  
  * Your delay is for an entire Workflow Execution  
  * Your delay is for a set time (e.g. 3pm Wednesdays)  
  * Your delay is recurring (though you can limit schedule runs to a single execution)

### ● Can I intentionally let a task queue backlog grow by underprovisioning workers for some period of time?

* Yes, **BUT**… be aware that Temporal does not guarantee FIFO. Task processing currently prioritizes sync match over async match.  Therefore a task on the backlog may be processed after a newer task is scheduled (when the newer task is able to sync match).  \[Issue [2517](https://github.com/temporalio/temporal/issues/2517) will provide config alternative when implemented\]

# Best Practices {#best-practices}

## Workflows {#workflows}

### ✔ Do they understand Workflow [**Determinism**](https://docs.temporal.io/workflows#deterministic-constraints) requirements? {#✔-do-they-understand-workflow-determinism-requirements?}

* A Workflow Definition can (and will) be executed many times.  Any re-execution must follow the same execution path for a given input to the Workflow Definition.  
* Have they encountered non-determinism errors (NDEs)?  
  * Are they unit testing using the Replayer with past Workflow histories?  
  * If NDEs occur due to code changes, see the Versioning section.  
* Ref: [community post about determinism](https://community.temporal.io/t/workflow-determinism/4027)

### ✔ Do they understand the [**Event History**](https://docs.temporal.io/workflows#event-history)? {#✔-do-they-understand-the-event-history?}

* A single execution has limits on the size of the Event History.  Are they at risk of hitting the limits?  
  * Is there a strategy to use [Continue-As-New](https://docs.temporal.io/workflows#continue-as-new) within a single workflow or use [Child Workflows](https://docs.temporal.io/workflows#child-workflow) to partition the work across multiple Workflows?  
* Temporal scales out incredibly well.  Partition work *across* Workflows rather than performing too much work *within* a single Workflow.

### ✔ Do they set [**Workflow Id**](https://docs.temporal.io/workflows#workflow-id) to a meaningful business identifier (or not)? {#✔-do-they-set-workflow-id-to-a-meaningful-business-identifier-(or-not)?}

* Are they leveraging Workflow Id uniqueness constraints for running Workflows?  
* Do they explicitly set a [Workflow Id Reuse Policy](https://docs.temporal.io/workflows#workflow-id-reuse-policy) for closed Workflows?  If so, why?  
* A Workflow Id cannot be reused for Open Workflows.   
  * It can effectively act as an idempotency key in designs where the Workflow may be started more than once.  
* Do not reuse the same Workflow Id with high frequency as it can result in server performance issues \[[ref](https://temporaltechnologies.slack.com/archives/C01H1G7J98F/p1702041104745229?thread_ts=1701805613.351819&cid=C01H1G7J98F)\]  
* In addition, the Workflow ***Run Id*** can change during a Workflow Execution (e.g. during Retry).    
  * Do *not* rely on Run Id for any logical choices in a Workflow, as this will lead to non-determinism issues.

### ✔ Do they explicitly set any **Workflow Timeout** options? {#✔-do-they-explicitly-set-any-workflow-timeout-options?}

* Generally, the defaults for Workflow Timeouts are sufficient. We do *not recommend changing the defaults*.  
  * Workflow Execution Timeout and Workflow Run Timeout default to infinite  
  * Workflow Task Timeout defaults to 10 seconds  
    * Potential reasons to increase WFT timeout:  
      * Time consuming data converters  
      * Large WF history  
    * Maximum value is 120 seconds  
    * Can be overridden at a namespace level via Dynamic Config via a support ticket  
    * Monitor CPU & Garbage Collection (if applicable) and the following SDK metrics  
      * workflow\_task\_execution\_latency  
      * request\_failure on the API RespondWorkflowTaskCompleted  
    * Possible causes of Workflow Task Timeouts include  
      * CPU going over 100%   
      * Starting a ton of async activities and combined inputs go over 4 MB  
      * Returning more than 2 MB of data  
    * Workflow tasks will retry with a backoff until a maximum retry interval of 10 minutes is reached \[[ref](https://temporaltechnologies.slack.com/archives/CTEFJ76QG/p1721703278870979)\]  
* If you need to timeout a Workflow, use explicit Timers within the Workflow, rather than setting exec or run timeouts.

### ✔ Do they understand **Workflow Failure** / error / exception handling? {#✔-do-they-understand-workflow-failure-/-error-/-exception-handling?}

* Any error that is **not** a [Temporal Failure](https://docs.temporal.io/references/failures) will fail the WF Task, which will be retried indefinitely with exponential backoff until it succeeds  
  * The exception is the Go SDK, where \`error\` fails the WF and \`panic\` fails the WFT 

### ✔ Do they set a **Workflow Retry** policy? {#✔-do-they-set-a-workflow-retry-policy?}

* Why do they?   
  * This is generally not recommended.  
  * By design, a Workflow should not fail due to intermittent issues

## Child Workflows {#child-workflows}

### ✔ Do they use [**Child Workflows**](https://docs.temporal.io/workflows#child-workflow)?  {#✔-do-they-use-child-workflows?}

* **Do** use Child Workflows strategically to:  
  * Partition large workloads into smaller chunks to stay under history size limits  
  * Target specific hosts (eg TaskQueue) due to security, workload profile, or other *strategic* reasons   
  * Extract behavior to simplify or explicitly define team ownership (eg [Shared Kernel](https://yoan-thirion.gitbook.io/knowledge-base/software-architecture/ddd-re-distilled#shared-kernel))  
* **Do not** use Child Workflows to:  
  * Organize code   
    * Use standard features of your programming language (e.g. packages, objects, structs, etc.) for code organization and modularity  
  * Reduce cost  
    * Child WFs will result in more events and actions than just using an Activity within the main WF (and on cloud Child WF counts as 2 actions)  
* [When to use a Child Workflow versus an Activity](https://docs.temporal.io/encyclopedia/child-workflows#when-to-use-child-workflows)  
  * **When in doubt, use an Activity**  
* [Valid reasons to use a Child Workflow](https://community.temporal.io/t/purpose-of-child-workflows/652/2)  
* As of July 2025, starting hundreds of child workflow per parent can cause multi-minute delays  
  * Details [here](https://temporaltechnologies.slack.com/archives/C03HNADRLKY/p1752262100562919?thread_ts=1752262034.541529&cid=C03HNADRLKY)

### ✔ **How many** Child WFs are being started by a Parent? {#✔-how-many-child-wfs-are-being-started-by-a-parent?}

* A single Parent SHOULD NOT start more than 1000 Children (per [docs](https://docs.temporal.io/workflows#when-to-use-child-workflows))  
  * **Note** that this is *not* a hardcoded limit though, but rather a code-smell.  
  * See [this thread](https://temporaltechnologies.slack.com/archives/C01RN061UMR/p1660750792060049?thread_ts=1660747402.058829&cid=C01RN061UMR) for a discussion where this “limit” was clarified as being *guidance*, not *absolute.*  
  * Also see this [discussion](https://temporaltechnologies.slack.com/archives/C03BY3HR2RH/p1669136625638709?thread_ts=1669121603.628799&cid=C03BY3HR2RH) where an alternative is proposed to remove the code-smell.   
  * Alternatives are proposed [here](https://community.temporal.io/t/batch-processing-vs-multiple-workflows/1688/2)

### ✔ Do the Parent and Child WF **need to share state**? {#✔-do-the-parent-and-child-wf-need-to-share-state?}

* If so, they can only communicate via Signals   
  * Local state cannot be shared

### ✔ Does the Parent **need to wait** on the Child Workflow result? {#✔-does-the-parent-need-to-wait-on-the-child-workflow-result?}

* Review the [Parent Close Policy](https://docs.temporal.io/encyclopedia/child-workflows#parent-close-policy) configuration

## Activities {#activities}

### ✔ Do they understand Activity [**Idempotency**](https://docs.temporal.io/activities#idempotency) as a best practice? {#✔-do-they-understand-activity-idempotency-as-a-best-practice?}

* An Activity Definition may be executed multiple times during failure scenarios.  
  * An Activity will only be executed *once* if it is successful, but has *at-least-once* semantics due to potential failure during execution  
*  We recommend using idempotency keys  
* See [https://temporal.io/blog/idempotency-and-durable-execution](https://temporal.io/blog/idempotency-and-durable-execution) 

### ✔ Are Activities **short-term or long**\-**running**? {#✔-are-activities-short-term-or-long-running?}

* Are [timeouts](#bookmark=id.omql4akvpemu) set appropriately for the Activity duration?  
* There is not a firm definition of “short” (perhaps a few minutes or less?)  
* “Long” running activities should [Heartbeat](https://docs.temporal.io/activities#activity-heartbeat)  
  * Use a short Heartbeat Timeout value  
  * Heartbeat frequently  
  * Include custom information/payload on the Heartbeat  
    * For saving progress.  
  * Do they understand that Heartbeats are [Throttled](https://docs.temporal.io/activities#throttling) by the SDK?  
  * An activity is a unit of failure detection (through timeouts), retries and visibility. It is OK to pack multiple operations in a single activity if you are OK with specifying a single timeout for all of them together and retrying them together. It also makes troubleshooting harder as it is less clear at which point the process is having issues. (source: Max in [community slack channel](https://temporalio.slack.com/archives/C04S80QKB2Q/p1711302488204919?thread_ts=1711297768.296809&cid=C04S80QKB2Q))  
* OK but what about *really long running (months)?*  
  * See [this excellent thread](https://temporaltechnologies.slack.com/archives/C04NYM5D3U6/p1757449292278879?thread_ts=1757447403.123839&cid=C04NYM5D3U6) in slack  
  * Heartbeats 👍, [signal-back-to workflow pattern](https://docs.temporal.io/activity-execution#when-to-use-async-completion), async completion

### ✔ What are the **Input and Output sizes** for Activity Payloads? {#✔-what-are-the-input-and-output-sizes-for-activity-payloads?}

* Is there risk of reaching the 2MB Blob Size Limit for Payloads?  
  * Or the History total size limit of 50MB?   
* Should they only pass references to the data, rather than the actual data?  
* Should they use sticky Activity queues (also known as “Worker-specific Activities”) to allow for the data to be stored locally and shared across Activities?  
* Are they compressing the payloads via a Data Converter?  
  * Compression is recommended by default.

### ✔ Is the Activity performing any **polling**? {#✔-is-the-activity-performing-any-polling?}

* [What is the best practice for a polling activity?](https://community.temporal.io/t/what-is-the-best-practice-for-a-polling-activity/328/2)  
* If polling interval is frequent, perform polling within the Activity using iteration  
* If polling interval is infrequent, then perform polling by using Retry Options  
* Can they use a Signal or [Async Activity Completion](https://docs.temporal.io/activities#asynchronous-activity-completion) approach instead?

### ✔ Is the Activity **listening** on a port or socket? {#✔-is-the-activity-listening-on-a-port-or-socket?}

* The listening process should be run outside the Workflow.   
  * Use Signal or SignalWithStart to start the Workflow from the listening process.

### ✔ What are the Activity **Timeout** settings? {#✔-what-are-the-activity-timeout-settings?}

* [Schedule-To-Start](https://docs.temporal.io/activities#schedule-to-start-timeout) should generally **not** be set (default is ♾️), but temporal\_activity\_schedule\_to\_start\_latency metric should be monitored. Schedule-to-start should only be used if workflow wants to take action in case worker(s) are busy or otherwise unavailable, for example when using host-specific task queues.  
* Either [Start-To-Close](https://docs.temporal.io/activities#start-to-close-timeout) or [Schedule-To-Close](https://docs.temporal.io/activities#schedule-to-close-timeout) **must** be set.  
  * Setting Start-To-Close is **strongly** recommended  
    * This timer resets on each retry  
  * Schedule-To-Close default is ♾️  
    * This timer is inclusive of all retries (i.e. it does **not** reset on each retry)  
* Each Activity should be individually considered for its own optimal timeout settings  
  * [One does not simply](https://www.dictionary.com/e/memes/one-does-not-simply/) use the same Timeout settings for every Activity in a WF  
* Do they know Activity Timeout is enforced on the server side?   
  * Timeout setting should be **greater** than the longest potential time in which the Activity would complete under normal circumstances.  
  * Or in other words they should have **shorter** timeout enforced on the worker side with upstream actions that may take longer to complete, e.g. DB write, API requests. If not this will lead to duplicate actions, and other resource contention when Activity Retry kicks in.

  [The 4 Types of Activity timeouts](https://temporal.io/blog/activity-timeouts)

### ✔ Do Activity Tasks run on Workers separate from the Workflow Tasks? {#✔-do-activity-tasks-run-on-workers-separate-from-the-workflow-tasks?}

* Do they require optimized compute resources or hardware? (e.g. CPU/Mem, GPUs)

### ✔ Do sequential Activities run on the same Worker (i.e. are they sticky)? {#✔-do-sequential-activities-run-on-the-same-worker-(i.e.-are-they-sticky)?}

* Are there large payloads that are used by multiple Activities that can’t or shouldn’t go through Temporal?

### ✔ Do they use [**Local Activities**](https://docs.temporal.io/activities#local-activity)? {#✔-do-they-use-local-activities?}

* **We recommend using regular Activities unless your use case requires very high throughput and large Activity fan outs of very short-lived Activities.**  
* How long is the Local Activity expected to run?  
  * Should not run for more than a few seconds, *inclusive of retries*  
* With LA, they lose the ability to rate limit & route tasks to workers  
* Reference: [Local Activity vs Activity](https://community.temporal.io/t/local-activity-vs-activity/290/3)  
* Reference: [Regular Activities vs Local Activities](https://docs.google.com/presentation/d/1BFipVEynxs5-fC7aeOsPO4xSeWan5L7-0WTdIi1DZU0/edit?slide=id.g36c5e7f8258_1_781#slide=id.g36c5e7f8258_1_781)

### ✔ What is the [**Activity Retry**](https://docs.temporal.io/retry-policies) policy? {#✔-what-is-the-activity-retry-policy?}

* Each Activity should be individually considered for its own optimal retry settings.  
* Do they use the default policy or set a custom policy?  
* Do they configure any specific [Non-Retryable](https://docs.temporal.io/retry-policies#non-retryable-errors) errors?  
* Reference: [https://docs.temporal.io/encyclopedia/detecting-activity-failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures)   
* Reference: [https://temporal.io/blog/failure-handling-in-practice](https://temporal.io/blog/failure-handling-in-practice) 

###  ✔ Do they understand **Activity Failure** / error / exception handling? {#✔-do-they-understand-activity-failure-/-error-/-exception-handling?}

* If an Activity Execution fails, the error is returned to the Workflow, which decides how to handle it.  
* Reference: [https://docs.temporal.io/references/failures](https://docs.temporal.io/references/failures) 

### ✔ Would an Activity need to receive [**Cancellation**](https://docs.temporal.io/activities#cancellation)? {#✔-would-an-activity-need-to-receive-cancellation?}

* If so, the Activity *must* Heartbeat (or be a Local Activity in Core-based SDKs only).

### ✔ Do they use too many Activities, or too few?

* Guidance here: [https://temporal.io/blog/how-many-activities-should-i-use-in-my-temporal-workflow](https://temporal.io/blog/how-many-activities-should-i-use-in-my-temporal-workflow) 

## Signals {#signals}

### ✔ Do they use [**Signals**](https://docs.temporal.io/workflows#signal)? {#✔-do-they-use-signals?}

* If not, should they be?    
  * Do they have a need to update the state of a Workflow during its execution?  
* If so, what is sending the Signal?  
  * Is it another Workflow or a Temporal Client in another application?

### ✔ Do they understand that Signals are… {#✔-do-they-understand-that-signals-are…}

* Recorded in the Event History (i.e. they will be replayed when a Workflow is replayed)  
* Delivered to a Workflow as part of the next scheduled Workflow Task  
  * Therefore, there may be some latency in delivery, depending on the current Workflow Task completing and the next being scheduled  
* Delivered in the order they are received  
* A single workflow can only handle a few signals per second (≤5/sec) and flooding with signals will result in not being able to continue-as-new / eventually hitting event limits of workflow history.

✔ Are they checking the return value or exceptions from sending a Signal? 

* A Signal call can give errors/throw exceptions:  
  * A workflow execution doesn’t exist  
  * A workflow execution is closed  
* See [Problems When Sending a Signal](https://docs.temporal.io/develop/java/message-passing#message-handler-troubleshooting), and SDK-specific info, for example [Java](https://docs.temporal.io/develop/java/message-passing#message-handler-troubleshooting):  
  * The Client can't contact the server: You'll receive a [WorkflowServiceException](https://javadoc.io/doc/io.temporal/temporal-sdk/latest/io/temporal/client/WorkflowServiceException.html) on which the cause is a [StatusRuntimeException](https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html) and status of UNAVAILABLE (after some retries).  
  * The Workflow does not exist: You'll receive a [WorkflowNotFoundException](https://javadoc.io/doc/io.temporal/temporal-sdk/latest/io/temporal/client/WorkflowNotFoundException.html).

### ✔ Are Signal handlers **idempotent** and **deterministic**? {#✔-are-signal-handlers-idempotent-and-deterministic?}

* It is possible (though unlikely) that a Signal could be delivered more than once ([reference](https://docs.temporal.io/encyclopedia/application-message-passing#footnote-label)).  
* Signal-handling code is Workflow code; therefore, it must adhere to the constraints of Workflow code (e.g. determinism)

### ✔ Is there a **high rate or volume** of Signals? {#✔-is-there-a-high-rate-or-volume-of-signals?}

* The recommended guidance is to limit signals to ≤5 per second sustained for optimal workflow performance  
  * The theoretical limit is tied to database latency (1/database\_latency), so \~20/sec with 50ms latency  
* All updates to a workflow are performed under a single lock, and workflows need resources for other operations beyond just signal processing \- executing workflow tasks, activities, etc., which all require database updates,   
  * so a high volume of Signals or Updates can prevent workflow progress  
* Is the total *volume* of Signals a concern for exceeding the Event History size limits?  
  * A single Workflow Execution is limited to 10000 Signals received in Temporal Cloud  
* Does the Workflow receiving the Signal also use Continue-As-New?  
  * Is there potential for the *rate* of Signals to be too fast for Continue-As-New to be successfully invoked?  
    * In order for CAN to run there must be a moment (\~100ms) when there are no unhandled Signals in the Workflow  
      * If the workflow cannot continue-as-new then the event history size increases until a WorkflowExecutionTerminated occurs with reason: Workflow History Size/count exceeds limit  
* Signals briefly lock a workflow execution, many signals to the same workflow can cause latency and limit throughput  
  * [Engineering/latency slack thread](https://temporaltechnologies.slack.com/archives/C02LGH6BG3A/p1684975109797379?thread_ts=1684963896.489869&cid=C02LGH6BG3A)  
  * [SA slack design discussion about signal volume](https://temporaltechnologies.slack.com/archives/C03HNADRLKY/p1752073205981859?thread_ts=1752008525.412319&cid=C03HNADRLKY).

### ✔ Do Signal handlers **invoke Activities**? {#✔-do-signal-handlers-invoke-activities?}

* This should be avoided.  Try to limit the scope of the Signal handler to updating the Workflow state.  Let the Workflow code react to state changes to invoke Activities.  
  * [Recommendation from Max in the Community Forum](https://community.temporal.io/t/signal-method-invocation-and-workflow-thread-safety/1679/2)

## Queries {#queries}

### ✔ Do they use [**Queries**](https://docs.temporal.io/workflows#query)? {#✔-do-they-use-queries?}

* If not, should they be?  
  * Are they manually storing or exporting Workflow state elsewhere during execution?  Why?

### ✔ Do they understand that Queries are… {#✔-do-they-understand-that-queries-are…}

* A synchronous operation  
* Available for both *running* and *completed* Workflows?  
  * Note: a Worker must be running and listening on the Task Queue

### ✔ Do Query handlers perform **read-only** operations? {#✔-do-query-handlers-perform-read-only-operations?}

* Queries must never mutate the state of a Workflow

## Update {#update}

### ✔ Do they use [**Update**](https://docs.temporal.io/workflows#update)? {#✔-do-they-use-update?}

* The Update handler function must be idempotent and deterministic  
* The handler function runs as part of the Workflow code and is subject to Workflow code constraints 

### ✔ Do they perform **Validation** on the Update request? {#✔-do-they-perform-validation-on-the-update-request?}

* This is optional (although recommended)  
  * Validation function cannot mutate the Workflow state  
    * Validators have the same basic restrictions as Queries  
* Updates that are rejected due to Validation are not recorded in the event history.

### ✔ Is it ok to invoke activities within the Update Handler? {#✔-is-it-ok-to-invoke-activities-within-the-update-handler?}

* [From  Maxim](https://temporaltechnologies.slack.com/archives/CTEFJ76QG/p1713543088839059?thread_ts=1709568936.227519&cid=CTEFJ76QG):  
  * Beside Java, it’s ok to invoke activities within the update handler. I advise Java due to how the system thread is per handler level.

### ✔ Do they use Early Return to reduce latency?

* See [Temporal Interactive Latency Options](https://docs.google.com/presentation/d/1dYU3lug3PdbliyEH2X_I9L2A27VA9CXfl5Jnw4t_azE/edit?pli=1&slide=id.g3129f517e9e_0_109#slide=id.g3129f517e9e_0_109) for techniques

## Workers & Task Queues {#workers-&-task-queues}

### ✔ How many Workers do they run? {#✔-how-many-workers-do-they-run?}

* Run at least 2 for high availability

### ✔ Do Workers register all Workflows and Activities that can be dispatched on the Task Queue? {#✔-do-workers-register-all-workflows-and-activities-that-can-be-dispatched-on-the-task-queue?}

* All Workers listening to a given Task Queue must have identical registrations of Activities and/or Workflows

### ✔ Do any Activities run on unique Task Queues? {#✔-do-any-activities-run-on-unique-task-queues?}

* Reasons to do so:  
  * Rate-limiting  
  * Activities that need to run on the same worker  
  * Special purpose workers, e.g. GPUs  
  * You can do this for [Differing priority for the work](https://community.temporal.io/t/activity-with-priorities/3398/5)  \- but see [TQ Priority and Fairness](https://docs.temporal.io/develop/task-queue-priority-fairness) for an easier way

### ✔ Do they configure rate-limiting for a Worker or Task Queue? {#✔-do-they-configure-rate-limiting-for-a-worker-or-task-queue?}

* Worker-side rate limiting  
  * \`maxConcurrentWorkflowTaskExecutionSize\`, \`maxConcurrentActivityExecutionSize\` and \`maxConcurrentLocalActivityExecutionSize\` define the number of total available slots for that Worker  
  * \`**maxWorkerActivitiesPerSecond**\`  
* Server-side rate limiting  
  * \`maxTaskQueueActivitiesPerSecond\`  
    * Must set the *same* value in each Worker that connects to the TQ  
* [Discussion about rate limiting options and best practices](https://community.temporal.io/t/rate-limit-configuration-and-best-practices/5498/2)  
* [Task Queue Activities per second architecture Diagram](https://lucid.app/lucidchart/408e3936-667b-435d-8fc3-603771ce0298/edit?invitationId=inv_465a4565-10e2-41af-b1a3-daa9280c43ac&page=0_0#)

### ✔ Do they require strict ordering for Tasks? {#✔-do-they-require-strict-ordering-for-tasks?}

* Without Priority and Fairness, Task Queues **do not** have any ordering guarantees  
  * For example, Tasks *across* separate WF executions may be executed in an order different than they were received.  
  * However, the order of executing *within* a single WF is fully controlled by the WF logic.  
  * Also, Signals for a single WF execution will always be delivered in the order they were received.  
* TQ [Priority and Fairness](https://docs.temporal.io/develop/task-queue-priority-fairness) allow for adjustments to how tasks are distributed in a task queue.   
  * [Priority](https://docs.temporal.io/develop/task-queue-priority-fairness#task-queue-priority) allows [Tasks](https://docs.temporal.io/tasks) to be executed in Priority order  
  * . [Fairness](https://docs.temporal.io/develop/task-queue-priority-fairness#task-queue-fairness) prevents one set of Tasks from blocking others within the same priority level.  
  * You can use Priority and Fairness individually or combine them to express Fairness within a Priority level.


### ✔ Can multiple workflows share one Task Queue? When should multiple task queues be used? {#✔-can-multiple-workflows-share-one-task-queue?-when-should-multiple-task-queues-be-used?}

* You can use multiple workflows per task queue  
* Reasons to use multiple queues:  
  * Multiple deployment units (services).   
  * Rate limiting and flow control.   
  * Routing to specific hosts.   
  * More info in [this community post from Maxim](https://community.temporal.io/t/in-what-situation-should-we-use-multiple-separated-task-queues/1254).

## Timers {#timers}

### ✔ What is the duration set for a timer or sleep? {#✔-what-is-the-duration-set-for-a-timer-or-sleep?}

* The shortest reliable duration is 1 second  
  * Anything less than one second may not be reliable

## Schedules {#schedules}

### ✔ Do they use [**Schedules**](https://docs.temporal.io/workflows#schedule)? {#✔-do-they-use-schedules?}

* How are they creating and managing the Schedule?  
  * SDK or CLI or Web UI?

### ✔ How many Schedules will they have? {#✔-how-many-schedules-will-they-have?}

* Will many run at the same time?  
  * Consider setting **Jitter** to offset execution, so they do not all run at once  
* What is the potential total Actions Per Second across all Schedules in a namespace?  
  * Temporal Cloud has a default limit of 10 (and can be raised to 100 with a short turnaround \- a couple of business days)  
    * If greater than 100, then the request to raise may take longer and should involve a discussion with Temporal engineering

### ✔ Are they using Schedules to achieve “delayed start”? {#✔-are-they-using-schedules-to-achieve-“delayed-start”?}

* As of 2024-02-20, [Start Delay](https://docs.temporal.io/workflows#delay-workflow-execution) is an experimental feature available in Go, Java and Python, and this should be considered.  
* Alternatively, they should use a normal Workflow with a timer/sleep at the beginning (instead of a Schedule with a run limit of 1).  
  * A normal Workflow will be cheaper for them, and easier to scale.

###  ✔ Will they need to [**Pause**](https://docs.temporal.io/workflows#pause) and/or [**Backfill**](https://docs.temporal.io/workflows#backfill) Schedules? {#✔-will-they-need-to-pause-and/or-backfill-schedules?}

* Will they allow Backfills to run in parallel (AllowAll overlap policy) or sequentially (BufferAll)?  
  * There is currently an (undocumented) limit of 1000 on the number of executions that can be Buffered.  They will need to batch or partition Backfill executions to stay under this limit.

### ✔ What is the configured [**Overlap Policy**](https://docs.temporal.io/workflows#overlap-policy)? {#✔-what-is-the-configured-overlap-policy?}

* The default is Skip (i.e. nothing happens; the Workflow Execution is not started)

### ✔ Do executions depend on [**Last Completion Result**](https://docs.temporal.io/workflows#last-completion-result)? {#✔-do-executions-depend-on-last-completion-result?}

* If their Overlap Policy allows overlaps, ensure they understand that the last completion means the run that successfully completed when the new run was started (and there may have been other executions started and currently running)

## Cron Jobs {#cron-jobs}

### ✔ Do they use [**Cron Jobs**](https://docs.temporal.io/workflows#temporal-cron-job)? {#✔-do-they-use-cron-jobs?}

* Why?  
  * Suggest using [Schedules](#schedules) instead

### ✔ Are they aware of Cron limitations? {#✔-are-they-aware-of-cron-limitations?}

* A Cron Workflow should not call Continue As New, as the cron schedule will be dropped/lost 

## Side Effects {#side-effects}

### ✔ Do they use [**Side Effects**](https://docs.temporal.io/workflow-execution/event#side-effect)? {#✔-do-they-use-side-effects?}

* If there is *any* chance the Side Effect could fail, use an Activity instead.  
* Side Effects are not implemented in Core SDKs. Use local activities instead.

## Data Converter {#data-converter}

### ✔ Do they use a custom [**Data Converter**](https://docs.temporal.io/dataconversion)? {#✔-do-they-use-a-custom-data-converter?}

* Why?   
  * For encryption, compression, custom serialization, etc?  
  * Compression is recommended  
* If used for encryption, is there a way to rotate keys?  
* Search Attribute values are *not* processed through a [custom Data Converter](https://docs.temporal.io/dataconversion#custom-data-converter).

### ✔ Are they using datetime/duration data types as input or return parameters? {#✔-are-they-using-datetime/duration-data-types-as-input-or-return-parameters?}

* The challenge with datetime/duration data types is that some languages like Python and Go don’t know how to natively serialize them. And JSON doesn’t support datetime/duration data type. That means it is up to every JSON library to determine if they implement a custom serialization for datetime/duration data types.  
* If you are using Python or Go, and/or plan on using multiple languages, you will need to implement your own custom Data Converter. This will require a common format that each SDK can convert back to its native datetime/duration data type.

### ✔ What latency is added by the custom Data Converter or Payload Codec? {#✔-what-latency-is-added-by-the-custom-data-converter-or-payload-codec?}

* Do these functions execute in the Workflow context, and contribute to Workflow Task Execution?    
  * Could they result in timeouts?

## Visibility {#visibility}

### ✔ Do they use [**Search Attributes**](https://docs.temporal.io/visibility#search-attribute)? {#✔-do-they-use-search-attributes?}

* These should be used for operational purposes only, and **not part of any business logic** in the Workflows   
  * For Workflows, use Queries or store the data in external datastore  
* They should not contain sensitive data, (e.g. PII, etc.) as SA values are not encoded by Data Converters or Payload Codecs  
* Are they storing too much data in Search Attributes?   
  * Is the data related to the execution of the Workflow?    
  * Are they going to be querying this data in the UI or the CLI?  
* Are they aware that Search Attribute updates are eventually consistent, i.e. there will be a delay (\~1-2 seconds) before the value is updated in the Visibility data store

### ✔ Do they use [**Memos**](https://docs.temporal.io/workflow-execution#memo)? {#✔-do-they-use-memos?}

* These are *eventually consistent*. The data may not be up-to-date when retrieved through the describe or list workflow operations.  
* Reference: [Memo vs Search Attributes vs Visibility Records](https://community.temporal.io/t/memo-vs-serach-attributes-vs-visibility-records/3003)

### ✔ Do they use any of the Visibility APIs within their app? {#✔-do-they-use-any-of-the-visibility-apis-within-their-app?}

* These are *eventually consistent*. The data may not be up-to-date.

## Versioning {#versioning}

### ✔ Do they (plan to) use [**Workflow Versioning**](https://docs.temporal.io/workflow-definition#workflow-versioning) (aka **Patch** Versioning) APIs within their app? {#✔-do-they-(plan-to)-use-workflow-versioning-(aka-patch-versioning)-apis-within-their-app?}

* Do they need to, or can they use [**Worker** Versioning](https://docs.temporal.io/worker-versioning) instead?  
  * *Don’t recommend yet as it is pre-release and the API is changing*  
  * Worker Versioning replaces **Task Queue** Versioning \- where you would have recommended TQ Versioning in the past, suggest Worker Versioning now.  
* When do they plan to remove the Versions / Patches?  
  * Never let the Versions / Patches accumulate indefinitely, it will lead to difficult to maintain code.  
* How long do the Workflows run for?  
  * They should have a plan to remove the Version/Patches after a finite period of time, e.g. 2 weeks or 30 days.  
  * Long-running Workflows need to keep patches in place until they either terminate or execute a Continue-As-New. Patches also need to be kept around if you plan on Querying them after they are closed. Patches should only be removed once the retention period has expired.   
  * For short running workflows, suggest **Worker** Versioning instead

### ✔ Do they **test** Version changes **using** the Workflow **Replayer**? {#✔-do-they-test-version-changes-using-the-workflow-replayer?}

* WorkflowReplayer in [Go](https://docs.temporal.io/dev-guide/go/testing#replay), [Java](https://docs.temporal.io/dev-guide/java/testing#replay)  
* worker.runReplayHistory in [TypeScript](https://docs.temporal.io/dev-guide/typescript/testing#replay)  
* replay\_workflow in [Python](https://docs.temporal.io/dev-guide/python/testing#replay)

## Continue As New

### ✔ Do they use Continue As New?

* With CAN just remember  
  * Timers won't be carried on, you have to calculate the remaining time and pass it to the new CAN execution, and there, schedule the timers.  
  * If Child Workflows are not started with parentClosePolicy ABANDON they will be terminated (or REQUEST\_CANCEL depending on the parentClosePolicy) when the parent workflow closes  
  * Ensure you wait for pending activities to complete (completed/failed..) before CAN  
  * Use Workflow.isEveryHandlerFinished() to ensure signals and update handlers have finished executing before CAN

## Interceptors {#interceptors}

Use Interceptors to change workflow and activity behavior at the worker level.  
For example, if you want some code that runs before every workflow starts, put that in a WorkflowInterceptor named “execute\_workflow”.   
Powerful, advanced, can cause confusing behavior if people forget they have interceptors enabled.

- [Blog that’s pretty good](https://platformatory.io/blog/Understanding-Temporal-Interceptors/)  
- [Java Workflow Interceptor docs](https://github.com/temporalio/sdk-java/blob/master/temporal-sdk/src/main/java/io/temporal/common/interceptors/WorkerInterceptor.java)

See Mike’s Github Project \- [https://github.com/temporalio/temporal-interceptor-seed](https://github.com/temporalio/temporal-interceptor-seed) 

## Sessions/Worker-Specific Task Queues {#sessions/worker-specific-task-queues}

Sessions are only available only in Go

* Note that when you use sessions, if the worker process dies, unless all the session activities have been processed, retry will call all of the session related activities again.

 Worker Specific Task Queues available in every SDK

* Use these to task Activities to specific workers

## Storage Optimization (Long Running Workflows) {#storage-optimization-(long-running-workflows)}

See [Cost Optimization \- Storage](https://docs.google.com/document/d/1CVsZ4kGHMI7X79HzZKPf2MDWBPbT4lpUDz-3yZOlE48/edit#heading=h.bsn3wts9qcyw), and in particular the section on [Continue As New](https://docs.google.com/document/d/1CVsZ4kGHMI7X79HzZKPf2MDWBPbT4lpUDz-3yZOlE48/edit#heading=h.spve8qj2rtv9) (also [this](https://docs.google.com/presentation/d/1mZvV53b49HaqPdj8fZnLTwcHZojDgbedclJYhak8d3o/edit#slide=id.g32c7d9e6922_0_232))

# Appendix {#appendix}

1. ## Best Practices Checklist {#best-practices-checklist}

[Workflows](#workflows)

[✔ Do they understand Workflow Determinism requirements?](#✔-do-they-understand-workflow-determinism-requirements?)

[✔ Do they understand the Event History?](#✔-do-they-understand-the-event-history?)

[✔ Do they set Workflow Id to a meaningful business identifier (or not)?](#✔-do-they-set-workflow-id-to-a-meaningful-business-identifier-\(or-not\)?)

[✔ Do they explicitly set any Workflow Timeout options?](#✔-do-they-explicitly-set-any-workflow-timeout-options?)

[✔ Do they understand Workflow Failure / error / exception handling?](#✔-do-they-understand-workflow-failure-/-error-/-exception-handling?)

[✔ Do they set a Workflow Retry policy?](#✔-do-they-set-a-workflow-retry-policy?)

[Child Workflows](#child-workflows)

[✔ Do they use Child Workflows?](#✔-do-they-use-child-workflows?)

[✔ How many Child WFs are being started by a Parent?](#✔-how-many-child-wfs-are-being-started-by-a-parent?)

[✔ Do the Parent and Child WF need to share state?](#✔-do-the-parent-and-child-wf-need-to-share-state?)

[✔ Does the Parent need to wait on the Child Workflow result?](#✔-does-the-parent-need-to-wait-on-the-child-workflow-result?)

[Activities](#activities)

[✔ Do they understand Activity Idempotency as a best practice?](#✔-do-they-understand-activity-idempotency-as-a-best-practice?)

[✔ Are Activities short-term or long-running?](#✔-are-activities-short-term-or-long-running?)

[✔ What are the Input and Output sizes for Activity Payloads?](#✔-what-are-the-input-and-output-sizes-for-activity-payloads?)

[✔ Is the Activity performing any polling?](#✔-is-the-activity-performing-any-polling?)

[✔ Is the Activity listening on a port or socket?](#✔-is-the-activity-listening-on-a-port-or-socket?)

[✔ What are the Activity Timeout settings?](#✔-what-are-the-activity-timeout-settings?)

[✔ Do Activity Tasks run on Workers separate from the Workflow Tasks?](#✔-do-activity-tasks-run-on-workers-separate-from-the-workflow-tasks?)

[✔ Do sequential Activities run on the same Worker (i.e. are they sticky)?](#✔-do-sequential-activities-run-on-the-same-worker-\(i.e.-are-they-sticky\)?)

[✔ Do they use Local Activities?](#✔-do-they-use-local-activities?)

[✔ What is the Activity Retry policy?](#✔-what-is-the-activity-retry-policy?)

[✔ Do they understand Activity Failure / error / exception handling?](#✔-do-they-understand-activity-failure-/-error-/-exception-handling?)

[✔ Would an Activity need to receive Cancellation?](#✔-would-an-activity-need-to-receive-cancellation?)

[Signals](#signals)

[✔ Do they use Signals?](#✔-do-they-use-signals?)

[✔ Do they understand that Signals are…](#✔-do-they-understand-that-signals-are…)

[✔ Are Signal handlers idempotent and deterministic?](#✔-are-signal-handlers-idempotent-and-deterministic?)

[✔ Is there a high rate or volume of Signals?](#✔-is-there-a-high-rate-or-volume-of-signals?)

[✔ Do Signal handlers invoke Activities?](#✔-do-signal-handlers-invoke-activities?)

[Queries](#queries)

[✔ Do they use Queries?](#✔-do-they-use-queries?)

[✔ Do they understand that Queries are…](#✔-do-they-understand-that-queries-are…)

[✔ Do Query handlers perform read-only operations?](#✔-do-query-handlers-perform-read-only-operations?)

[Update](#update)

[✔ Do they use Update?](#✔-do-they-use-update?)

[✔ Do they perform Validation on the Update request?](#✔-do-they-perform-validation-on-the-update-request?)

[✔ Is it ok to invoke activities within the Update Handler?](#✔-is-it-ok-to-invoke-activities-within-the-update-handler?)

[Workers & Task Queues](#workers-&-task-queues)

[✔ How many Workers do they run?](#✔-how-many-workers-do-they-run?)

[✔ Do Workers register all Workflows and Activities that can be dispatched on the Task Queue?](#✔-do-workers-register-all-workflows-and-activities-that-can-be-dispatched-on-the-task-queue?)

[✔ Do any Activities run on unique Task Queues?](#✔-do-any-activities-run-on-unique-task-queues?)

[✔ Do they configure rate-limiting for a Worker or Task Queue?](#✔-do-they-configure-rate-limiting-for-a-worker-or-task-queue?)

[✔ Do they require strict ordering for Tasks?](#✔-do-they-require-strict-ordering-for-tasks?)

[✔ Can multiple workflows share one Task Queue? When should multiple task queues be used?](#✔-can-multiple-workflows-share-one-task-queue?-when-should-multiple-task-queues-be-used?)

[Timers](#timers)

[✔ What is the duration set for a timer or sleep?](#✔-what-is-the-duration-set-for-a-timer-or-sleep?)

[Schedules](#schedules)

[✔ Do they use Schedules?](#✔-do-they-use-schedules?)

[✔ How many Schedules will they have?](#✔-how-many-schedules-will-they-have?)

[✔ Are they using Schedules to achieve “delayed start”?](#✔-are-they-using-schedules-to-achieve-“delayed-start”?)

[✔ Will they need to Pause and/or Backfill Schedules?](#✔-will-they-need-to-pause-and/or-backfill-schedules?)

[✔ What is the configured Overlap Policy?](#✔-what-is-the-configured-overlap-policy?)

[✔ Do executions depend on Last Completion Result?](#✔-do-executions-depend-on-last-completion-result?)

[Cron Jobs](#cron-jobs)

[✔ Do they use Cron Jobs?](#✔-do-they-use-cron-jobs?)

[✔ Are they aware of Cron limitations?](#✔-are-they-aware-of-cron-limitations?)

[Side Effects](#side-effects)

[✔ Do they use Side Effects?](#✔-do-they-use-side-effects?)

[Data Converter](#data-converter)

[✔ Do they use a custom Data Converter?](#✔-do-they-use-a-custom-data-converter?)

[✔ Are they using datetime/duration data types as input or return parameters?](#✔-are-they-using-datetime/duration-data-types-as-input-or-return-parameters?)

[✔ What latency is added by the custom Data Converter or Payload Codec?](#✔-what-latency-is-added-by-the-custom-data-converter-or-payload-codec?)

[Visibility](#visibility)

[✔ Do they use Search Attributes?](#✔-do-they-use-search-attributes?)

[✔ Do they use Memos?](#✔-do-they-use-memos?)

[✔ Do they use any of the Visibility APIs within their app?](#✔-do-they-use-any-of-the-visibility-apis-within-their-app?)

[Versioning](#versioning)

[✔ Do they (plan to) use Workflow Versioning (aka Patch Versioning) APIs within their app?](#✔-do-they-\(plan-to\)-use-workflow-versioning-\(aka-patch-versioning\)-apis-within-their-app?)

[✔ Do they test Version changes using the Workflow Replayer?](#✔-do-they-test-version-changes-using-the-workflow-replayer?)

2. ## Useful Links:  {#useful-links:}

- [Common Design Patterns](https://taonic.github.io/temporal-design-patterns/)  
- [SDK Features Matrix](https://www.notion.so/temporalio/d86479c52be643c6a7c5f22cef5807e4?v=46d47ff7e32643dbb29950136fb3e5cd)  
- [Temporal 102 deck, covers a lot of topics, with diagrams](https://www.google.com/url?q=https://docs.google.com/presentation/d/1DsK9ZE-XHpLac2jBTf29UUSol1HBghCjzV8-3PRWT7Y/edit?slide%3Did.g2d0bcd56d06_0_392%23slide%3Did.g2d0bcd56d06_0_392&sa=D&source=docs&ust=1752690322127243&usg=AOvVaw1pdJ2swgV1ikET-UTTSmyD) that are discussed above  
- [Temporal Python Troubleshooting Guide](https://github.com/temporalio/dev-success/blob/main/python/troubleshooting_guide.md#the-thread-inside-an-async-def-python-function-is-blocked)  
- [Notes for Code Reviews](https://docs.google.com/document/d/1RiFq1ExYvjNqdLvI_Suuo8rGS1DhqQy3SgrOLcwWaqQ/edit?tab=t.0#heading=h.abk8gemnj13u) \- the Code Review companion guide