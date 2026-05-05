from dataclasses import dataclass

TASK_QUEUE = "webhooks-task-queue"
WORKFLOW_ID_PREFIX = "order"


@dataclass
class OrderInput:
    order_id: str
    amount: float


@dataclass
class PaymentPayload:
    payment_id: str
    amount: float
