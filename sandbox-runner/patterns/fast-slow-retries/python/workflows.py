from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    import activities


@workflow.defn
class FastSlowRetryWorkflow:
    @workflow.run
    async def run(self, request: str) -> str:
        # Phase 1: fast retries — short interval, bounded attempt count.
        # Recovers from transient errors within seconds.
        fast_policy = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=1.0,
            maximum_interval=timedelta(seconds=1),
            maximum_attempts=5,
        )
        try:
            return await workflow.execute_activity(
                activities.call_downstream,
                args=[request, 1],
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=fast_policy,
            )
        except ActivityError:
            workflow.logger.warning(
                "Fast retries exhausted — switching to slow retry phase",
                extra={"request": request},
            )

        # Phase 2: slow retries — long fixed interval, unlimited attempts.
        # Waits patiently for an extended outage to resolve.
        slow_policy = RetryPolicy(
            initial_interval=timedelta(seconds=3),
            backoff_coefficient=1.0,
            # maximum_attempts not set — unlimited
        )
        return await workflow.execute_activity(
            activities.call_downstream,
            args=[request, 2],
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=slow_policy,
        )
