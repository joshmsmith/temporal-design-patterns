from datetime import timedelta
from typing import Optional

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import process_payment, send_webhook_callback
    from shared import CallbackInput, OrderInput, PaymentPayload


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


@workflow.defn
class DelayedCallbackWorkflow:
    @workflow.run
    async def run(self, input: CallbackInput) -> str:
        workflow.logger.info(
            f"Sleeping {input.delay_seconds}s before calling {input.callback_url}"
        )

        # Durable sleep — survives worker restarts, server restarts, everything
        await workflow.sleep(timedelta(seconds=input.delay_seconds))

        # Fire the outbound callback; Temporal retries on HTTP failure
        result = await workflow.execute_activity(
            send_webhook_callback,
            input,
            start_to_close_timeout=timedelta(minutes=5),
        )
        workflow.logger.info(f"Callback delivered to {input.callback_url}")
        return result
