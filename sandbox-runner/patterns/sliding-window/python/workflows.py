from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ParentClosePolicy, continue_as_new

from activities import process_record
from shared import COMPLETION_SIGNAL, TASK_QUEUE, WINDOW_SIZE, SlidingWindowInput


@workflow.defn
class RecordProcessorWorkflow:
    """Child workflow: processes one record and signals the parent on completion."""

    @workflow.run
    async def run(self, record_id: str, parent_workflow_id: str) -> None:
        await workflow.execute_activity(
            process_record,
            record_id,
            start_to_close_timeout=timedelta(seconds=30),
        )
        workflow.logger.info(f"Processed record: {record_id}")

        # Signal the parent that this slot is now free.
        # Ignore if the parent has already completed (final run finished before us).
        parent = workflow.get_external_workflow_handle(parent_workflow_id)
        try:
            await parent.signal(COMPLETION_SIGNAL, record_id)
        except ApplicationError as e:
            if "not found" in str(e).lower():
                workflow.logger.info(f"Parent already completed, signal not needed: {record_id}")
            else:
                raise


@workflow.defn
class SlidingWindowWorkflow:
    """Parent workflow: maintains a fixed window of concurrent child workflows.
    Calls continue_as_new after dispatching window_size children."""

    def __init__(self) -> None:
        self._pending_signals = 0
        self._total_processed = 0

    @workflow.signal(name=COMPLETION_SIGNAL)
    def record_completed(self, record_id: str) -> None:
        self._pending_signals += 1
        self._total_processed += 1

    @workflow.run
    async def run(self, input: SlidingWindowInput) -> int:
        # Use += so any completions that signal before run() starts are preserved.
        self._total_processed += input.total_processed
        record_ids = input.record_ids
        window_size = input.window_size
        start_index = input.start_index
        in_flight = input.in_flight
        parent_id = workflow.info().workflow_id
        next_index = start_index
        dispatched = 0
        active = in_flight

        # Only start (window_size - in_flight) new children. Carried-over in-flight
        # children from the previous run will signal us when they complete.
        new_fill = min(window_size - in_flight, len(record_ids) - start_index)
        for _ in range(new_fill):
            await workflow.start_child_workflow(
                RecordProcessorWorkflow.run,
                args=[record_ids[next_index], parent_id],
                id=f"{parent_id}/record-{record_ids[next_index]}",
                task_queue=TASK_QUEUE,
                parent_close_policy=ParentClosePolicy.ABANDON,
            )
            next_index += 1
            dispatched += 1
            active += 1

        # If the window is full after the initial fill, continue-as-new immediately
        # so the parent doesn't wait before handing off to the next run.
        if dispatched >= window_size:
            workflow.logger.info(f"ContinueAsNew: nextIndex={next_index} totalProcessed={self._total_processed}")
            continue_as_new(args=[SlidingWindowInput(
                record_ids=record_ids,
                window_size=window_size,
                start_index=next_index,
                total_processed=self._total_processed,
                in_flight=window_size,
            )])
            return

        # Slide the window.
        while next_index < len(record_ids):
            await workflow.wait_condition(lambda: self._pending_signals > 0)
            self._pending_signals -= 1
            active -= 1
            await workflow.start_child_workflow(
                RecordProcessorWorkflow.run,
                args=[record_ids[next_index], parent_id],
                id=f"{parent_id}/record-{record_ids[next_index]}",
                task_queue=TASK_QUEUE,
                parent_close_policy=ParentClosePolicy.ABANDON,
            )
            next_index += 1
            dispatched += 1
            active += 1

            if dispatched >= window_size:
                workflow.logger.info(f"ContinueAsNew: nextIndex={next_index} totalProcessed={self._total_processed}")
                # Pass next_index as the next unstarted record; in_flight=window_size
                # because the window is always full at CAN time.
                continue_as_new(args=[SlidingWindowInput(
                    record_ids=record_ids,
                    window_size=window_size,
                    start_index=next_index,
                    total_processed=self._total_processed,
                    in_flight=window_size,
                )])
                return

        # Wait for all remaining in-flight children to complete.
        await workflow.wait_condition(lambda: self._pending_signals >= active)
        workflow.logger.info(f"Sliding window complete: total={len(record_ids)} totalProcessed={self._total_processed}")
        return self._total_processed
