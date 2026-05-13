from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.workflow import ChildWorkflowHandle

from activities import process_leaf
from shared import LEAF_THRESHOLD, MAX_DEPTH, RESULT_SIGNAL, TASK_QUEUE


@dataclass
class ResultPayload:
    id: str
    results: list[str]


@workflow.defn
class LeafWorkflow:
    """Leaf workflow: processes one record and signals the result back to parent."""

    @workflow.run
    async def run(self, record: str, parent_workflow_id: str) -> None:
        result = await workflow.execute_activity(
            process_leaf,
            record,
            start_to_close_timeout=timedelta(seconds=30),
        )
        workflow.logger.info(f"Leaf processed: {record} → {result}")

        parent = workflow.get_external_workflow_handle(parent_workflow_id)
        await parent.signal(RESULT_SIGNAL, ResultPayload(id=record, results=[result]))


@workflow.defn
class NodeWorkflow:
    """Node workflow: splits or fans out to leaves, collects results via signals,
    and signals aggregated results up to its parent."""

    def __init__(self) -> None:
        self._collected: list[str] = []
        self._received = 0

    @workflow.signal(name=RESULT_SIGNAL)
    def node_result(self, payload: ResultPayload) -> None:
        self._collected.extend(payload.results)
        self._received += 1

    @workflow.run
    async def run(
        self,
        records: list[str],
        depth: int = 0,
        parent_workflow_id: str = "",
    ) -> list[str]:
        if depth > MAX_DEPTH:
            raise RuntimeError(f"Tree depth exceeded {MAX_DEPTH}")

        my_id = workflow.info().workflow_id
        expected = 0

        if len(records) <= LEAF_THRESHOLD:
            # Fan out to leaf workflows — one per record.
            expected = len(records)
            for record in records:
                await workflow.start_child_workflow(
                    LeafWorkflow.run,
                    args=[record, my_id],
                    id=f"{my_id}/leaf-{record}",
                    task_queue=TASK_QUEUE,
                )
        else:
            # Split and recurse into child node workflows.
            mid = len(records) // 2
            chunks = [records[:mid], records[mid:]]
            expected = len(chunks)
            for i, chunk in enumerate(chunks):
                await workflow.start_child_workflow(
                    NodeWorkflow.run,
                    args=[chunk, depth + 1, my_id],
                    id=f"{my_id}/node-d{depth+1}-{i}",
                    task_queue=TASK_QUEUE,
                )

        await workflow.wait_condition(lambda: self._received >= expected)

        workflow.logger.info(
            f"Node complete: depth={depth} records={len(records)} results={len(self._collected)}"
        )

        if parent_workflow_id:
            parent = workflow.get_external_workflow_handle(parent_workflow_id)
            await parent.signal(
                RESULT_SIGNAL, ResultPayload(id=my_id, results=self._collected)
            )

        return self._collected
