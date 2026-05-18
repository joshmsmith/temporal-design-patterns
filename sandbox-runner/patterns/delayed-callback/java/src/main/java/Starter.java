import io.temporal.client.BatchRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;

public class Starter {
    public static void main(String[] args) throws Exception {
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);

        // --- Pattern 1: Inbound Webhook via Signal-with-Start ---
        System.out.println("=== Pattern 1: Inbound Webhook via Signal-with-Start ===");
        String orderId = String.valueOf(System.currentTimeMillis());
        String workflowId = Shared.WORKFLOW_ID_PREFIX + "-" + orderId;

        OrderWorkflow workflow = client.newWorkflowStub(
                OrderWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(Shared.TASK_QUEUE)
                        .setWorkflowId(workflowId)
                        .build());

        Shared.PaymentPayload payment = new Shared.PaymentPayload(
                "pay-" + System.currentTimeMillis(), 99.99);

        System.out.println("Sending webhook for order: " + workflowId);

        // Signal-with-Start: atomically starts the workflow (if not running) and
        // delivers the payment signal — this is exactly what your HTTP handler would do.
        BatchRequest request = client.newSignalWithStartRequest();
        request.add(workflow::run, new Shared.OrderInput(orderId, 99.99));
        request.add(workflow::paymentReceived, payment);
        client.signalWithStart(request);
        System.out.println("Webhook signal sent: " + payment.paymentId());

        String result = WorkflowStub.fromTyped(workflow).getResult(String.class);
        System.out.println("Order completed: " + result);
        System.out.println("Search '" + workflowId + "' in the Temporal UI to inspect the history.");

        // --- Pattern 2: Delayed Outbound Callback ---
        System.out.println("\n=== Pattern 2: Delayed Outbound Callback ===");
        String callbackId = Shared.CALLBACK_WORKFLOW_ID_PREFIX + "-" + System.currentTimeMillis();
        Shared.CallbackInput callbackInput = new Shared.CallbackInput(
                "https://httpbin.org/post",
                "hello from workflow " + callbackId,
                5);

        DelayedCallbackWorkflow cbWorkflow = client.newWorkflowStub(
                DelayedCallbackWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(Shared.TASK_QUEUE)
                        .setWorkflowId(callbackId)
                        .build());

        System.out.println("Starting delayed callback workflow: " + callbackId);
        System.out.println("Will POST to " + callbackInput.callbackUrl() + " after " + callbackInput.delaySeconds() + "s durable sleep");

        WorkflowClient.start(cbWorkflow::run, callbackInput);
        String cbResult = WorkflowStub.fromTyped(cbWorkflow).getResult(String.class);
        System.out.println("Callback result: " + cbResult);
        System.out.println("Search '" + callbackId + "' in the Temporal UI to inspect the history.");

        System.exit(0);
    }
}
