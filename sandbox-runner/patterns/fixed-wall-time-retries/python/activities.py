import asyncio

from temporalio import activity


@activity.defn
async def authorize_transaction(transaction_id: str) -> str:
    info = activity.info()
    activity.logger.info(
        f"Authorizing transaction {transaction_id} (attempt {info.attempt}) "
        "— simulating a slow downstream service"
    )
    # Simulate a service that takes longer than the 5s StartToCloseTimeout.
    # Each attempt will be cancelled by Temporal, consuming the time budget.
    # Change the sleep to 1 second to watch a successful authorization.
    await asyncio.sleep(10)
    return f"authorized-{transaction_id}"
