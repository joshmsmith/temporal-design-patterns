from dataclasses import dataclass

TASK_QUEUE = "resumable-activity-task-queue"
WORKFLOW_ID_PREFIX = "resumable-activity"


@dataclass
class TransferInput:
    from_account: str
    to_account: str
    amount: float
