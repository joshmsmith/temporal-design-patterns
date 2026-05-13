import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.workflow import ChildWorkflowHandle

from activities import process_record
from shared import CHUNK_SIZE, TASK_QUEUE


@workflow.defn
class RecordBatchWorkflow:
    """Child workflow: processes a contiguous slice of records [offset, offset+length)."""

    @workflow.run
    async def run(self, offset: int, length: int) -> int:
        processed = 0
        for i in range(offset, offset + length):
            await workflow.execute_activity(
                process_record,
                i,
                start_to_close_timeout=timedelta(seconds=10),
            )
            processed += 1
        workflow.logger.info(f"Batch complete: offset={offset} length={length} processed={processed}")
        return processed


@workflow.defn
class FanOutWorkflow:
    """Parent workflow: splits the total record set into chunks and fans out to
    one child workflow per chunk."""

    @workflow.run
    async def run(self, total_records: int, chunk_size: int = CHUNK_SIZE) -> int:
        parent_id = workflow.info().workflow_id
        handles: list[ChildWorkflowHandle] = []

        offset = 0
        while offset < total_records:
            length = min(chunk_size, total_records - offset)
            handle = await workflow.start_child_workflow(
                RecordBatchWorkflow.run,
                args=[offset, length],
                id=f"{parent_id}/batch-{offset}",
                task_queue=TASK_QUEUE,
            )
            handles.append(handle)
            offset += chunk_size

        results = await asyncio.gather(*handles)
        total = sum(results)
        workflow.logger.info(
            f"Fan-out complete: totalRecords={total_records} chunks={len(handles)} total={total}"
        )
        return total
