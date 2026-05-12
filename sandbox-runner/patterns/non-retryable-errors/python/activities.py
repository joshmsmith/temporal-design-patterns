from temporalio import activity
from temporalio.exceptions import ApplicationError


@activity.defn
async def process_order(order_id: str) -> str:
    info = activity.info()
    activity.logger.info(f"Processing order {order_id} (attempt {info.attempt})")

    # Permanent failure — non-retryable.
    # Temporal delivers this to the Workflow immediately without further retries.
    if order_id.startswith("INVALID-"):
        raise ApplicationError(
            f"Order {order_id} not found in database",
            type="OrderNotFoundError",
            non_retryable=True,
        )

    # Transient failure — retryable.
    # Temporal retries this automatically until it succeeds (attempt 3).
    if info.attempt < 3:
        raise RuntimeError(
            f"database connection timeout (attempt {info.attempt}) — will retry"
        )

    return f"Order {order_id} processed successfully on attempt {info.attempt}"
