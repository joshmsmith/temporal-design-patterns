import asyncio

from temporalio import activity

from shared import TOTAL_RECORDS


@activity.defn
async def fetch_page(offset: int, page_size: int) -> list[int]:
    end = min(offset + page_size, TOTAL_RECORDS)
    return list(range(offset, end))


@activity.defn
async def process_record(record_id: int) -> None:
    # Simulate processing work.
    await asyncio.sleep(0.05)
