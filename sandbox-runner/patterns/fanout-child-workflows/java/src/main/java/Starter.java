import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;

public class Starter {
    public static void main(String[] args) {
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);

        String workflowId = Shared.WORKFLOW_ID_PREFIX + "-" + System.currentTimeMillis();
        FanOutWorkflow.Parent workflow = client.newWorkflowStub(
                FanOutWorkflow.Parent.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(Shared.TASK_QUEUE)
                        .setWorkflowId(workflowId)
                        .build());

        System.out.println("Started workflow: " + workflowId);
        System.out.println("Processing " + Shared.TOTAL_RECORDS + " records in chunks of " + Shared.CHUNK_SIZE + "…");
        int total = workflow.run(Shared.TOTAL_RECORDS, Shared.CHUNK_SIZE);
        System.out.println("Fan-out complete: processed " + total + " records");
        System.out.println(
                "Open the Temporal UI and search for '" + workflowId
                        + "' to see the parent and child workflows.");
    }
}
