import asyncio
import time

from temporalio.client import Client

from shared import CALLBACK_WORKFLOW_ID_PREFIX, TASK_QUEUE, WORKFLOW_ID_PREFIX, CallbackInput, OrderInput, PaymentPayload
from workflows import DelayedCallbackWorkflow, OrderWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")

    # --- Pattern 1: Inbound Webhook via Signal-with-Start ---
    order_id = f"{int(time.time() * 1000)}"
    workflow_id = f"{WORKFLOW_ID_PREFIX}-{order_id}"
    order = OrderInput(order_id=order_id, amount=99.99)
    payment = PaymentPayload(
        payment_id=f"pay-{int(time.time() * 1000)}",
        amount=99.99,
    )

    print("=== Pattern 1: Inbound Webhook via Signal-with-Start ===")
    print(f"Sending webhook for order: {workflow_id}")

    # Signal-with-Start: atomically starts the workflow (if not running) and
    # delivers the payment signal — this is exactly what your HTTP handler would do.
    handle = await client.start_workflow(
        OrderWorkflow.run,
        order,
        id=workflow_id,
        task_queue=TASK_QUEUE,
        start_signal="payment_received",
        start_signal_args=[payment],
    )
    print(f"Webhook signal sent: {payment.payment_id}")

    result = await handle.result()
    print(f"Order completed: {result}")
    print(f"Search '{workflow_id}' in the Temporal UI to inspect the history.")

    # --- Pattern 2: Delayed Outbound Callback ---
    print("\n=== Pattern 2: Delayed Outbound Callback ===")
    callback_id = f"{CALLBACK_WORKFLOW_ID_PREFIX}-{int(time.time() * 1000)}"
    callback_input = CallbackInput(
        callback_url="https://httpbin.org/post",
        payload=f"hello from workflow {callback_id}",
        delay_seconds=5,
    )

    print(f"Starting delayed callback workflow: {callback_id}")
    print(f"Will POST to {callback_input.callback_url} after {callback_input.delay_seconds}s durable sleep")

    cb_handle = await client.start_workflow(
        DelayedCallbackWorkflow.run,
        callback_input,
        id=callback_id,
        task_queue=TASK_QUEUE,
    )

    cb_result = await cb_handle.result()
    print(f"Callback result: {cb_result}")
    print(f"Search '{callback_id}' in the Temporal UI to inspect the history.")


if __name__ == "__main__":
    asyncio.run(main())
