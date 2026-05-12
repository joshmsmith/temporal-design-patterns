from temporalio import activity
from temporalio.exceptions import ApplicationError

from shared import TransferInput

# Valid accounts for the demo — any other account number fails.
VALID_ACCOUNTS = {"account-123", "account-456", "account-789"}


@activity.defn
async def execute_transfer(transfer: TransferInput) -> None:
    info = activity.info()
    activity.logger.info(
        f"Executing transfer: {transfer.amount} from {transfer.from_account} "
        f"to {transfer.to_account} (attempt {info.attempt})"
    )

    if transfer.to_account not in VALID_ACCOUNTS:
        raise ApplicationError(
            f"Account {transfer.to_account} not found",
            type="AccountNotFoundError",
            non_retryable=True,
        )

    activity.logger.info(
        f"Transfer of {transfer.amount} to {transfer.to_account} completed"
    )
