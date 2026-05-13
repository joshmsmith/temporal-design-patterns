import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@WorkflowInterface
public interface FanOutWorkflow {

    /** Parent workflow: fans out to one child per chunk. */
    @WorkflowInterface
    interface Parent {
        @WorkflowMethod
        int run(int totalRecords, int chunkSize);
    }

    /** Child workflow: processes a contiguous slice [offset, offset+length). */
    @WorkflowInterface
    interface Child {
        @WorkflowMethod
        int run(int offset, int length);
    }

    final class ParentImpl implements Parent {
        @Override
        public int run(int totalRecords, int chunkSize) {
            if (chunkSize <= 0) chunkSize = Shared.CHUNK_SIZE;

            String parentId = Workflow.getInfo().getWorkflowId();
            List<Promise<Integer>> promises = new ArrayList<>();

            for (int offset = 0; offset < totalRecords; offset += chunkSize) {
                int length = Math.min(chunkSize, totalRecords - offset);
                ChildWorkflowOptions opts = ChildWorkflowOptions.newBuilder()
                        .setWorkflowId(parentId + "/batch-" + offset)
                        .setTaskQueue(Shared.TASK_QUEUE)
                        .build();
                Child child = Workflow.newChildWorkflowStub(Child.class, opts);
                promises.add(Async.function(child::run, offset, length));
            }

            int total = 0;
            for (Promise<Integer> p : promises) {
                total += p.get();
            }
            Workflow.getLogger(ParentImpl.class)
                    .info("Fan-out complete: totalRecords={} chunks={} total={}", totalRecords, promises.size(), total);
            return total;
        }
    }

    final class ChildImpl implements Child {
        private final Activities activities = Workflow.newActivityStub(
                Activities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(10))
                        .build());

        @Override
        public int run(int offset, int length) {
            int processed = 0;
            for (int i = offset; i < offset + length; i++) {
                activities.processRecord(i);
                processed++;
            }
            Workflow.getLogger(ChildImpl.class)
                    .info("Batch complete: offset={} length={} processed={}", offset, length, processed);
            return processed;
        }
    }
}
