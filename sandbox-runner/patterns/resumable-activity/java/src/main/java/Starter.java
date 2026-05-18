import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;

public class Starter {
    public static void main(String[] args) throws InterruptedException {
        // Register the TransferStatus search attribute (idempotent — ignored if already exists).
        try {
            new ProcessBuilder("temporal", "operator", "search-attribute", "create",
                    "--name", "TransferStatus", "--type", "Keyword")
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .redirectError(ProcessBuilder.Redirect.DISCARD)
                    .start()
                    .waitFor();
        } catch (Exception ignored) {}

        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);

        String workflowId = Shared.WORKFLOW_ID_PREFIX + "-" + System.currentTimeMillis();

        Shared.TransferInput transfer = new Shared.TransferInput(
                "account-001",
                "WRONG-ACCOUNT-999",  // Invalid — will fail the first attempt
                500.00);

        System.out.println("Starting transfer workflow: " + workflowId);
        System.out.println("  From:   " + transfer.fromAccount());
        System.out.println("  To:     " + transfer.toAccount() + "  <- invalid account number");
        System.out.printf("  Amount: $%.2f%n", transfer.amount());
        System.out.println();

        TransferWorkflow workflow = client.newWorkflowStub(
                TransferWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(Shared.TASK_QUEUE)
                        .setWorkflowId(workflowId)
                        .build());

        // Start asynchronously so we can send signals while it runs
        WorkflowClient.start(workflow::run, transfer);

        // Use an untyped stub for queries and signals after start
        WorkflowStub stub = client.newUntypedWorkflowStub(workflowId);

        // Wait for the workflow to park in AWAITING_CORRECTION
        System.out.println("Waiting for workflow to fail and park...");
        waitForStatus(stub, "AWAITING_CORRECTION");
        System.out.println("Workflow is now AWAITING_CORRECTION — parked in Temporal, no polling, zero cost.");
        System.out.println();

        // Simulate an operator correcting the account number
        String correctedAccount = "account-123";
        System.out.println("Sending correction signal with corrected account: " + correctedAccount);
        stub.signal("retryWithCorrection", correctedAccount);

        // Wait for the workflow to park in AWAITING_APPROVAL
        waitForStatus(stub, "AWAITING_APPROVAL");
        System.out.println("Transfer succeeded. Workflow is now AWAITING_APPROVAL.");
        System.out.println();

        // Simulate the client approving the transfer
        System.out.println("Sending approval signal: approved=true");
        stub.signal("approve", true);

        String result = stub.getResult(String.class);
        System.out.println("Workflow completed: " + result);
        System.out.println();
        System.out.println("Open the Temporal UI and search for '" + workflowId
                + "' to see the full signal history.");
    }

    private static void waitForStatus(WorkflowStub stub, String targetStatus)
            throws InterruptedException {
        while (true) {
            String status = stub.query("getStatus", String.class);
            if (targetStatus.equals(status)) {
                return;
            }
            Thread.sleep(500);
        }
    }
}
