from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    import activities


@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order_id: str) -> str:
        try:
            return await workflow.execute_activity(
                activities.process_order,
                order_id,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(
                    maximum_attempts=5,
                    initial_interval=timedelta(seconds=1),
                    backoff_coefficient=1.0,
                ),
            )
        except ActivityError as e:
            return f"Order {order_id} failed: {e.cause}"
