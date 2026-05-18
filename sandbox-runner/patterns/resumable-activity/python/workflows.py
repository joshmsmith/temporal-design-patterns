from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy, SearchAttributeKey
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    import activities
    from shared import TransferInput

TRANSFER_STATUS_KEY = SearchAttributeKey.for_keyword("TransferStatus")


@workflow.defn
class TransferWorkflow:
    def __init__(self) -> None:
        self._status = "PENDING"
        self._corrected_account: str | None = None
        self._approval: bool | None = None

    @workflow.run
    async def run(self, transfer: TransferInput) -> str:
        account = transfer.to_account
        correction_attempts = 0

        while True:
            self._status = "TRANSFERRING"
            try:
                await workflow.execute_activity(
                    activities.execute_transfer,
                    TransferInput(transfer.from_account, account, transfer.amount),
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=RetryPolicy(maximum_attempts=1),
                )
                break  # Activity succeeded — exit the correction loop
            except ActivityError:
                correction_attempts += 1
                if correction_attempts > 5:
                    self._status = "FAILED"
                    workflow.upsert_search_attributes([TRANSFER_STATUS_KEY.value_set(self._status)])
                    raise
                self._status = "AWAITING_CORRECTION"
                workflow.upsert_search_attributes([TRANSFER_STATUS_KEY.value_set(self._status)])
                workflow.logger.warning(
                    "Transfer failed — waiting for account correction",
                    extra={"to_account": account},
                )
                # Park until the admin sends a correction signal
                await workflow.wait_condition(
                    lambda: self._corrected_account is not None
                )
                account = self._corrected_account  # type: ignore[assignment]
                self._corrected_account = None

        self._status = "AWAITING_APPROVAL"
        await workflow.wait_condition(lambda: self._approval is not None)

        if self._approval:
            self._status = "COMPLETED"
            return f"Transfer of {transfer.amount} to {account} completed"
        self._status = "REJECTED"
        return "Transfer rejected by client"

    @workflow.signal
    def retry_with_correction(self, corrected_account: str) -> None:
        self._corrected_account = corrected_account

    @workflow.signal
    def approve(self, approved: bool) -> None:
        self._approval = approved

    @workflow.query
    def get_status(self) -> str:
        return self._status
