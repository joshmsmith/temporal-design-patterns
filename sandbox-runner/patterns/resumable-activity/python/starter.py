import asyncio
import time

from temporalio.client import Client

from shared import TASK_QUEUE, WORKFLOW_ID_PREFIX, TransferInput
from workflows import TransferWorkflow


async def wait_for_status(handle: object, target_status: str) -> None:
    """Poll the workflow status query until the target state is reached."""
    while True:
        status = await handle.query(TransferWorkflow.get_status)  # type: ignore[attr-defined]
        if status == target_status:
            return
        await asyncio.sleep(0.5)


async def main() -> None:
    client = await Client.connect("localhost:7233")
    workflow_id = f"{WORKFLOW_ID_PREFIX}-{int(time.time() * 1000)}"

    transfer = TransferInput(
        from_account="account-001",
        to_account="WRONG-ACCOUNT-999",  # Invalid — will fail the first attempt
        amount=500.00,
    )

    print(f"Starting transfer workflow: {workflow_id}")
    print(f"  From: {transfer.from_account}")
    print(f"  To:   {transfer.to_account}  ← invalid account number")
    print(f"  Amount: ${transfer.amount:.2f}")
    print()

    handle = await client.start_workflow(
        TransferWorkflow.run,
        transfer,
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )

    # Wait for the workflow to park in AWAITING_CORRECTION
    print("Waiting for workflow to exhaust retries and park...")
    await wait_for_status(handle, "AWAITING_CORRECTION")
    print("Workflow is now AWAITING_CORRECTION — parked in Temporal, no polling, zero cost.")
    print()

    # Simulate an operator correcting the account number
    corrected_account = "account-123"
    print(f"Sending correction signal with corrected account: {corrected_account}")
    await handle.signal(TransferWorkflow.retry_with_correction, corrected_account)

    # Wait for the workflow to park in AWAITING_APPROVAL
    await wait_for_status(handle, "AWAITING_APPROVAL")
    print("Transfer succeeded. Workflow is now AWAITING_APPROVAL.")
    print()

    # Simulate the client approving the transfer
    print("Sending approval signal: approved=True")
    await handle.signal(TransferWorkflow.approve, True)

    result = await handle.result()
    print(f"Workflow completed: {result}")
    print()
    print(f"Open the Temporal UI and search for '{workflow_id}' to see the full signal history.")


if __name__ == "__main__":
    asyncio.run(main())
