import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public interface NodeWorkflow {

    /** Node workflow (and root): splits or fans out, collects results, signals parent. */
    @WorkflowInterface
    interface Node {
        @WorkflowMethod
        List<String> run(List<String> records, int depth, String parentWorkflowId);

        @SignalMethod
        void nodeResult(String id, List<String> results);
    }

    /** Leaf workflow: processes one record and signals the parent node. */
    @WorkflowInterface
    interface Leaf {
        @WorkflowMethod
        void run(String record, String parentWorkflowId);
    }

    final class NodeImpl implements Node {
        private final List<String> collected = new ArrayList<>();
        private int received = 0;

        @Override
        public void nodeResult(String id, List<String> results) {
            collected.addAll(results);
            received++;
        }

        @Override
        public List<String> run(List<String> records, int depth, String parentWorkflowId) {
            if (depth > Shared.MAX_DEPTH) {
                throw new RuntimeException("Tree depth exceeded " + Shared.MAX_DEPTH);
            }

            String myId = Workflow.getInfo().getWorkflowId();
            int expected;

            if (records.size() <= Shared.LEAF_THRESHOLD) {
                // Fan out to leaf workflows — one per record.
                expected = records.size();
                for (String record : records) {
                    ChildWorkflowOptions opts = ChildWorkflowOptions.newBuilder()
                            .setWorkflowId(myId + "/leaf-" + record)
                            .setTaskQueue(Shared.TASK_QUEUE)
                            .build();
                    Leaf leaf = Workflow.newChildWorkflowStub(Leaf.class, opts);
                    Async.procedure(leaf::run, record, myId);
                }
            } else {
                // Split and recurse into child node workflows.
                int mid = records.size() / 2;
                List<List<String>> chunks = List.of(
                        records.subList(0, mid),
                        records.subList(mid, records.size()));
                expected = chunks.size();
                for (int i = 0; i < chunks.size(); i++) {
                    ChildWorkflowOptions opts = ChildWorkflowOptions.newBuilder()
                            .setWorkflowId(String.format("%s/node-d%d-%d", myId, depth + 1, i))
                            .setTaskQueue(Shared.TASK_QUEUE)
                            .build();
                    Node child = Workflow.newChildWorkflowStub(Node.class, opts);
                    Async.function(child::run, chunks.get(i), depth + 1, myId);
                }
            }

            // Wait for all expected signals.
            final int exp = expected;
            Workflow.await(() -> received >= exp);

            Workflow.getLogger(NodeImpl.class)
                    .info("Node complete: depth={} records={} results={}", depth, records.size(), collected.size());

            // Signal aggregated results up to parent (if not root).
            if (parentWorkflowId != null && !parentWorkflowId.isEmpty()) {
                ExternalWorkflowStub parent = Workflow.newUntypedExternalWorkflowStub(parentWorkflowId, "");
                parent.signal(Shared.RESULT_SIGNAL, myId, new ArrayList<>(collected));
            }

            return collected;
        }
    }

    final class LeafImpl implements Leaf {
        private final Activities activities = Workflow.newActivityStub(
                Activities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(30))
                        .build());

        @Override
        public void run(String record, String parentWorkflowId) {
            String result = activities.processLeaf(record);
            Workflow.getLogger(LeafImpl.class)
                    .info("Leaf processed: {} → {}", record, result);

            ExternalWorkflowStub parent = Workflow.newUntypedExternalWorkflowStub(parentWorkflowId, "");
            parent.signal(Shared.RESULT_SIGNAL, record, List.of(result));
        }
    }
}
