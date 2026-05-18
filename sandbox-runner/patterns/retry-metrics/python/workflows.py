from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    import activities


@workflow.defn
class MonitoredRetryWorkflow:
    @workflow.run
    async def run(self, endpoint: str) -> str:
        return await workflow.execute_activity(
            activities.call_downstream_service,
            endpoint,
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=1.0,
                # No maximum_attempts — retries until success.
                # The metric counter alerts on-call before the SLA is breached.
            ),
        )
