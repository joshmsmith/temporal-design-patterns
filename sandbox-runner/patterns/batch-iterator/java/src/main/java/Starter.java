import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;

public class Starter {
    public static void main(String[] args) {
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);

        String workflowId = Shared.WORKFLOW_ID_PREFIX + "-" + System.currentTimeMillis();
        BatchIteratorWorkflow workflow = client.newWorkflowStub(
                BatchIteratorWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(Shared.TASK_QUEUE)
                        .setWorkflowId(workflowId)
                        .build());

        System.out.println("Started workflow: " + workflowId);
        System.out.println("Processing " + Shared.TOTAL_RECORDS + " records (page size " + Shared.PAGE_SIZE + ")…");
        int total = workflow.run(0, 0);
        System.out.println("Batch iterator complete: processed " + total + " records");
        System.out.println(
                "Open the Temporal UI and search for '" + workflowId
                        + "' to see the Continue-As-New chain.");
    }
}
