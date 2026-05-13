from dataclasses import dataclass, field

TASK_QUEUE = "sliding-window-task-queue"
WORKFLOW_ID_PREFIX = "sliding-window"
WINDOW_SIZE = 3
RECORD_IDS = [f"record-{i}" for i in range(12)]
COMPLETION_SIGNAL = "recordCompleted"


@dataclass
class SlidingWindowInput:
    """Input for SlidingWindowWorkflow. Bundled as a single object so all runs
    (including continues-as-new) share one consistent argument shape."""
    record_ids: list
    window_size: int = WINDOW_SIZE
    start_index: int = 0
    total_processed: int = 0
    in_flight: int = 0
