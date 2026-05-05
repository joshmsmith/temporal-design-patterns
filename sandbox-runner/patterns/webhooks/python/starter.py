import asyncio
import time

from temporalio.client import Client

from shared import TASK_QUEUE, WORKFLOW_ID_PREFIX, OrderInput, PaymentPayload
from workflows import OrderWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")

    order_id = f"{int(time.time() * 1000)}"
    workflow_id = f"{WORKFLOW_ID_PREFIX}-{order_id}"
    order = OrderInput(order_id=order_id, amount=99.99)

    print(f"Starting order workflow: {workflow_id}")

    # Start the workflow — it will block waiting for the payment_received signal
    handle = await client.start_workflow(
        OrderWorkflow.run,
        order,
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )

    # Simulate the inbound webhook arriving 3 seconds later.
    # In production this would be your HTTP handler calling signal_workflow.
    print("Simulating inbound payment webhook in 3 seconds...")
    await asyncio.sleep(3)

    payment = PaymentPayload(
        payment_id=f"pay-{int(time.time() * 1000)}",
        amount=99.99,
    )

    # This is what your HTTP /webhook handler does upon receiving the POST:
    await client.get_workflow_handle(workflow_id).signal(
        OrderWorkflow.payment_received, payment
    )
    print(f"Webhook signal sent: {payment.payment_id}")

    result = await handle.result()
    print(f"Order completed: {result}")
    print(f"Open the Temporal UI and search for '{workflow_id}' to see the history.")


if __name__ == "__main__":
    asyncio.run(main())
