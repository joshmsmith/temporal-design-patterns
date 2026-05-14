from dataclasses import dataclass

TASK_QUEUE = "accumulator-task-queue"
WORKFLOW_ID_PREFIX = "accumulator"


@dataclass
class OrderItem:
    order_id: str
    item_id: str
    name: str
