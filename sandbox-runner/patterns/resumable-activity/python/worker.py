import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from activities import execute_transfer
from shared import TASK_QUEUE
from workflows import TransferWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[TransferWorkflow],
        activities=[execute_transfer],
    )
    print(f"Worker listening on task queue '{TASK_QUEUE}'")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
