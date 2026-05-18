import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from activities import authorize_transaction
from shared import TASK_QUEUE
from workflows import PaymentAuthWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[PaymentAuthWorkflow],
        activities=[authorize_transaction],
    )
    print(f"Worker listening on task queue '{TASK_QUEUE}'")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
