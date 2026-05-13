from datetime import timedelta

from temporalio import workflow
from temporalio.workflow import continue_as_new

from activities import fetch_page, process_record
from shared import PAGE_SIZE


@workflow.defn
class BatchIteratorWorkflow:
    """Processes PAGE_SIZE records per run, then calls continue_as_new with the
    next offset so history stays bounded."""

    @workflow.run
    async def run(self, offset: int = 0, total_processed: int = 0) -> int:
        page: list[int] = await workflow.execute_activity(
            fetch_page,
            args=[offset, PAGE_SIZE],
            start_to_close_timeout=timedelta(seconds=10),
        )

        for record_id in page:
            await workflow.execute_activity(
                process_record,
                record_id,
                start_to_close_timeout=timedelta(seconds=10),
            )
            total_processed += 1

        workflow.logger.info(
            f"Processed page: offset={offset} pageSize={len(page)} totalProcessed={total_processed}"
        )

        if len(page) == PAGE_SIZE:
            # More pages remain — continue as new with the next offset.
            continue_as_new(args=[offset + PAGE_SIZE, total_processed])

        # Reached here only on the final (partial) page.
        return total_processed
