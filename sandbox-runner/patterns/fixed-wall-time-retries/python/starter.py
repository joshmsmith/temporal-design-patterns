import asyncio
import time

from temporalio.client import Client

from shared import TASK_QUEUE, WORKFLOW_ID_PREFIX
from workflows import PaymentAuthWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    txn_id = f"txn-{int(time.time() * 1000)}"

    print(f"Starting payment authorization for {txn_id}")
    print("The downstream service is slow — each attempt exceeds the 5s per-attempt timeout.")
    print("The 12s total budget will be exhausted after ~2 attempts.")
    print()

    handle = await client.start_workflow(
        PaymentAuthWorkflow.run,
        txn_id,
        id=f"{WORKFLOW_ID_PREFIX}-{txn_id}",
        task_queue=TASK_QUEUE,
    )

    result = await handle.result()
    print(f"Workflow result: {result}")
    print()
    print(f"Open the Temporal UI and search for '{handle.id}' to inspect the timeout history.")


if __name__ == "__main__":
    asyncio.run(main())
