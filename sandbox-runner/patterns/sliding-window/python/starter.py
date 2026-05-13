import asyncio
import time

from temporalio.client import Client

from shared import RECORD_IDS, TASK_QUEUE, WINDOW_SIZE, WORKFLOW_ID_PREFIX, SlidingWindowInput
from workflows import SlidingWindowWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    workflow_id = f"{WORKFLOW_ID_PREFIX}-{int(time.time() * 1000)}"
    handle = await client.start_workflow(
        SlidingWindowWorkflow.run,
        args=[SlidingWindowInput(record_ids=RECORD_IDS)],
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )
    print(f"Started workflow: {workflow_id}")
    print(f"Processing {len(RECORD_IDS)} records with window size {WINDOW_SIZE}…")

    total_processed = await handle.result()
    print(f"Sliding window complete: processed {total_processed} records")
    print(
        f"Open the Temporal UI and search for '{workflow_id}' to see the parent and child workflows."
    )


if __name__ == "__main__":
    asyncio.run(main())
