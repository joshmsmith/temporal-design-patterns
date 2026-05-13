import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;

import java.util.HashSet;

public class Starter {
    public static void main(String[] args) {
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);

        String workflowId = Shared.WORKFLOW_ID_PREFIX + "-" + System.currentTimeMillis();
        SlidingWindowWorkflow.Parent workflow = client.newWorkflowStub(
                SlidingWindowWorkflow.Parent.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(Shared.TASK_QUEUE)
                        .setWorkflowId(workflowId)
                        .build());

        System.out.println("Started workflow: " + workflowId);
        System.out.println("Processing " + Shared.RECORD_IDS.size()
                + " records with window size " + Shared.WINDOW_SIZE + "…");
        int totalProcessed = workflow.run(
                new Shared.SlidingWindowInput(Shared.RECORD_IDS, Shared.WINDOW_SIZE, 0, 0, new HashSet<>()));
        System.out.println("Sliding window complete: processed " + totalProcessed + " records");
        System.out.println(
                "Open the Temporal UI and search for '" + workflowId
                        + "' to see the parent and child workflows.");
    }
}
