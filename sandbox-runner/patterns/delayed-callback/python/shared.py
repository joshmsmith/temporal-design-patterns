from dataclasses import dataclass

TASK_QUEUE = "webhooks-task-queue"
WORKFLOW_ID_PREFIX = "order"
CALLBACK_WORKFLOW_ID_PREFIX = "delayed-callback"


@dataclass
class OrderInput:
    order_id: str
    amount: float


@dataclass
class PaymentPayload:
    payment_id: str
    amount: float


@dataclass
class CallbackInput:
    callback_url: str
    payload: str
    delay_seconds: int
