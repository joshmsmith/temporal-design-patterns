from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, TimeoutError, TimeoutType

with workflow.unsafe.imports_passed_through():
    import activities


@workflow.defn
class PaymentAuthWorkflow:
    @workflow.run
    async def run(self, transaction_id: str) -> str:
        try:
            return await workflow.execute_activity(
                activities.authorize_transaction,
                transaction_id,
                schedule_to_close_timeout=timedelta(seconds=12),  # total budget
                start_to_close_timeout=timedelta(seconds=5),       # per attempt
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    backoff_coefficient=1.0,
                ),
            )
        except ActivityError as e:
            cause = e.__cause__
            if isinstance(cause, TimeoutError) and cause.type == TimeoutType.SCHEDULE_TO_CLOSE:
                workflow.logger.error(
                    "Authorization failed — 12-second SLA breached",
                    extra={"transaction_id": transaction_id},
                )
            return f"Transaction {transaction_id}: SLA breached — authorization failed"
