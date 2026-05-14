from collections import deque
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import process_items
    from shared import OrderItem


@workflow.defn
class AccumulatorWorkflow:
    """
    Accumulator workflow: collects order items sent via signals, deduplicates
    them, and processes them together after a sliding inactivity timeout.

    Workflow ID should be deterministic per bucket key so Signal-With-Start
    routes all signals for the same order to the same running instance.
    """

    def __init__(self) -> None:
        # Buffer for signals received before the main loop drains them
        self._unprocessed: deque[OrderItem] = deque()
        self._exit_requested = False

    @workflow.signal(name="add-item")
    async def add_item(self, item: OrderItem) -> None:
        # Signal handlers just buffer — no activity calls inside handlers
        self._unprocessed.append(item)

    @workflow.signal(name="exit")
    async def exit_workflow(self) -> None:
        self._exit_requested = True

    @workflow.run
    async def run(
        self,
        bucket_key: str,
        accumulated: list[OrderItem] | None = None,
        seen_keys: list[str] | None = None,
    ) -> str:
        # Restore state carried forward from a previous Continue-as-New run
        items: list[OrderItem] = list(accumulated or [])
        seen_set: set[str] = set(seen_keys or [])

        while True:
            # Sliding window: block until a signal arrives or the timer expires
            timed_out = not await workflow.wait_condition(
                lambda: bool(self._unprocessed) or self._exit_requested,
                timeout=timedelta(seconds=10),
            )

            # Drain and deduplicate the signal queue
            while self._unprocessed:
                item = self._unprocessed.popleft()
                if item.order_id == bucket_key and item.item_id not in seen_set:
                    seen_set.add(item.item_id)
                    items.append(item)

            if timed_out or self._exit_requested:
                result = await workflow.execute_activity(
                    process_items,
                    args=[bucket_key, items],
                    start_to_close_timeout=timedelta(seconds=10),
                )
                workflow.logger.info(
                    f"Processed order batch: {bucket_key} with {len(items)} items"
                )
                if not self._unprocessed:
                    return result
                # More signals arrived after timeout/exit — loop to process them

            if workflow.info().is_continue_as_new_suggested():
                # History growing large — continue as new, carrying state forward
                workflow.logger.info(f"Continuing as new: {bucket_key}")
                workflow.continue_as_new(args=[bucket_key, items, list(seen_set)])
