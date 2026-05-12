from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    import activities


@workflow.defn
class PaymentWorkflow:
    @workflow.run
    async def run(self, order_id: str) -> str:
        try:
            return await workflow.execute_activity(
                activities.charge_payment_api,
                order_id,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(
                    maximum_attempts=3,
                    initial_interval=timedelta(seconds=1),
                    backoff_coefficient=1.0,
                ),
            )
        except ActivityError as e:
            workflow.logger.error(
                "All 3 payment attempts failed — escalating to on-call team",
                extra={"order_id": order_id},
            )
            return f"Order {order_id}: payment failed after 3 attempts — escalated to on-call"
