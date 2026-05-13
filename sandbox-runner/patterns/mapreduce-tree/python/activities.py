import asyncio

from temporalio import activity


@activity.defn
async def process_leaf(record: str) -> str:
    # Simulate processing and return a result string.
    await asyncio.sleep(0.05)
    return f"processed({record})"
