import asyncio
import time

from temporalio.client import Client

from shared import TASK_QUEUE, WORKFLOW_ID_PREFIX
from workflows import PaymentWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    order_id = f"order-{int(time.time() * 1000)}"

    print(f"Starting payment workflow for {order_id}")
    print("The payment gateway is down — all 3 attempts will fail.")
    print()

    handle = await client.start_workflow(
        PaymentWorkflow.run,
        order_id,
        id=f"{WORKFLOW_ID_PREFIX}-{order_id}",
        task_queue=TASK_QUEUE,
    )

    result = await handle.result()
    print(f"Workflow result: {result}")
    print()
    print(f"Open the Temporal UI and search for '{handle.id}' to inspect the retry history.")


if __name__ == "__main__":
    asyncio.run(main())
