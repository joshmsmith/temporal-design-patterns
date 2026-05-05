from datetime import timedelta
from typing import Optional

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import process_payment
    from shared import OrderInput, PaymentPayload


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self._payment: Optional[PaymentPayload] = None

    @workflow.run
    async def run(self, order: OrderInput) -> str:
        workflow.logger.info(f"Order {order.order_id}: waiting for payment webhook")

        # Block until the inbound webhook signal arrives (or timeout after 24 hours)
        await workflow.wait_condition(
            lambda: self._payment is not None,
            timeout=timedelta(hours=24),
        )

        if self._payment is None:
            return f"Order {order.order_id}: timed out waiting for payment"

        result = await workflow.execute_activity(
            process_payment,
            self._payment,
            start_to_close_timeout=timedelta(seconds=30),
        )
        return result

    @workflow.signal
    async def payment_received(self, payload: PaymentPayload) -> None:
        workflow.logger.info(f"Payment signal received: {payload.payment_id}")
        self._payment = payload
