import asyncio

from temporalio import activity


@activity.defn
async def process_record(record_id: int) -> None:
    # Simulate processing work.
    await asyncio.sleep(0.05)
