import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;

public class Starter {
    public static void main(String[] args) throws Exception {
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);

        String orderId = String.valueOf(System.currentTimeMillis());
        String workflowId = Shared.WORKFLOW_ID_PREFIX + "-" + orderId;

        OrderWorkflow workflow = client.newWorkflowStub(
                OrderWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(Shared.TASK_QUEUE)
                        .setWorkflowId(workflowId)
                        .build());

        System.out.println("Starting order workflow: " + workflowId);

        // Start the workflow asynchronously — it will block waiting for the signal
        WorkflowClient.start(workflow::run, new Shared.OrderInput(orderId, 99.99));

        // Simulate the inbound webhook arriving 3 seconds later.
        // In production this would be your HTTP handler calling signalWorkflow.
        System.out.println("Simulating inbound payment webhook in 3 seconds...");
        Thread.sleep(3000);

        Shared.PaymentPayload payment = new Shared.PaymentPayload(
                "pay-" + System.currentTimeMillis(), 99.99);

        // This is what your HTTP /webhook handler does upon receiving the POST:
        workflow.paymentReceived(payment);
        System.out.println("Webhook signal sent: " + payment.paymentId());

        // Wait for the workflow to complete
        String result = WorkflowStub.fromTyped(workflow).getResult(String.class);
        System.out.println("Order completed: " + result);
        System.out.println("Open the Temporal UI and search for '" + workflowId + "' to see the history.");

        System.exit(0);
    }
}
