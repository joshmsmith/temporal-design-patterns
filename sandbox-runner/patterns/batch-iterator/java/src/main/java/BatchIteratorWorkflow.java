import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;
import java.util.List;

@WorkflowInterface
public interface BatchIteratorWorkflow {
    @WorkflowMethod
    int run(int offset, int totalProcessed);

    final class Impl implements BatchIteratorWorkflow {
        private final Activities activities = Workflow.newActivityStub(
                Activities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(10))
                        .build());

        @Override
        public int run(int offset, int totalProcessed) {
            List<Integer> page = activities.fetchPage(offset, Shared.PAGE_SIZE);

            for (int recordId : page) {
                activities.processRecord(recordId);
                totalProcessed++;
            }

            Workflow.getLogger(BatchIteratorWorkflow.class)
                    .info("Processed page: offset={} pageSize={} totalProcessed={}",
                            offset, page.size(), totalProcessed);

            if (page.size() == Shared.PAGE_SIZE) {
                // More pages remain — continue as new with the next offset.
                BatchIteratorWorkflow next = Workflow.newContinueAsNewStub(BatchIteratorWorkflow.class);
                next.run(offset + Shared.PAGE_SIZE, totalProcessed);
            }

            // Reached here only on the final (partial) page.
            return totalProcessed;
        }
    }
}
