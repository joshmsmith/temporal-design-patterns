import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;

import java.util.List;

public class Starter {
    public static void main(String[] args) {
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);

        String workflowId = Shared.WORKFLOW_ID_PREFIX + "-" + System.currentTimeMillis();
        NodeWorkflow.Node workflow = client.newWorkflowStub(
                NodeWorkflow.Node.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(Shared.TASK_QUEUE)
                        .setWorkflowId(workflowId)
                        .build());

        System.out.println("Started workflow: " + workflowId);
        System.out.println("Processing " + Shared.RECORDS.size() + " records via MapReduce Tree…");
        List<String> results = workflow.run(Shared.RECORDS, 0, "");
        System.out.println("MapReduce Tree complete: " + results.size() + " results");
        System.out.println("Results: " + String.join(", ", results));
        System.out.println(
                "Open the Temporal UI and search for '" + workflowId
                        + "' to see the full workflow tree.");
    }
}
