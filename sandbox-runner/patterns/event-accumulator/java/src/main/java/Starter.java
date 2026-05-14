import io.temporal.client.BatchRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;

import java.util.ArrayList;
import java.util.List;

public class Starter {
    public static void main(String[] args) {
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);

        // Items arriving for two separate orders from multiple producers.
        // item-1 is sent twice to demonstrate deduplication.
        List<Shared.OrderItem> items = List.of(
                new Shared.OrderItem("order-A", "item-1", "Widget"),
                new Shared.OrderItem("order-B", "item-2", "Gadget"),
                new Shared.OrderItem("order-A", "item-3", "Gizmo"),
                new Shared.OrderItem("order-B", "item-4", "Doohickey"),
                new Shared.OrderItem("order-A", "item-1", "Widget"),   // duplicate — ignored
                new Shared.OrderItem("order-A", "item-5", "Thingamajig")
        );

        for (Shared.OrderItem item : items) {
            String workflowId = Shared.WORKFLOW_ID_PREFIX + "-" + item.orderId;
            AccumulatorWorkflow stub = client.newWorkflowStub(
                    AccumulatorWorkflow.class,
                    WorkflowOptions.newBuilder()
                            .setTaskQueue(Shared.TASK_QUEUE)
                            .setWorkflowId(workflowId)
                            .build());

            // Signal-With-Start: start if not running, otherwise just deliver the signal
            BatchRequest request = client.newSignalWithStartRequest();
            request.add(stub::accumulate, item.orderId, new ArrayList<>(), new ArrayList<>());
            request.add(stub::addItem, item);
            client.signalWithStart(request);

            System.out.println("Signaled " + workflowId + ": " + item.name + " (" + item.itemId + ")");
        }

        System.out.println("\nWaiting for workflows to process their items...");
        for (String orderId : new String[]{"order-A", "order-B"}) {
            String workflowId = Shared.WORKFLOW_ID_PREFIX + "-" + orderId;
            // Connect to the running workflow by ID and wait for its result
            AccumulatorWorkflow handle = client.newWorkflowStub(AccumulatorWorkflow.class, workflowId);
            String result = handle.accumulate(orderId, new ArrayList<>(), new ArrayList<>());
            System.out.println(workflowId + " result: " + result);
        }
        System.out.println("\nOpen the Temporal UI and search for '" + Shared.WORKFLOW_ID_PREFIX
                + "' to see the accumulator workflows.");
    }
}
