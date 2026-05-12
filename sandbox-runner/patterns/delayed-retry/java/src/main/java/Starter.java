import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;

public class Starter {
    public static void main(String[] args) {
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);

        String workflowId = Shared.WORKFLOW_ID_PREFIX + "-" + System.currentTimeMillis();

        System.out.println("Starting rate-limited API workflow.");
        System.out.println("Attempt 1 will receive a simulated HTTP 429 with Retry-After: 5s.");
        System.out.println(
                "The nextRetryDelay overrides the RetryPolicy interval for that single retry.");
        System.out.println();

        RateLimitedWorkflow workflow = client.newWorkflowStub(
                RateLimitedWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(Shared.TASK_QUEUE)
                        .setWorkflowId(workflowId)
                        .build());

        System.out.println("Started workflow: " + workflowId);

        String result = workflow.run("https://api.example.com/data");
        System.out.println("Workflow result: " + result);
        System.out.println();
        System.out.println("Open the Temporal UI and search for '" + workflowId
                + "' to see the retry timing.");
    }
}
