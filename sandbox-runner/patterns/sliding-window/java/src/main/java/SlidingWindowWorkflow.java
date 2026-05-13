import io.temporal.activity.ActivityOptions;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.workflow.*;

import java.time.Duration;
import java.util.*;

public interface SlidingWindowWorkflow {

    /** Parent workflow: maintains a fixed window of concurrent child workflows. */
    @WorkflowInterface
    interface Parent {
        @WorkflowMethod
        int run(Shared.SlidingWindowInput input);

        @SignalMethod
        void recordCompleted(String recordId);
    }

    /** Child workflow: processes one record and signals the parent on completion. */
    @WorkflowInterface
    interface Child {
        @WorkflowMethod
        void run(String recordId, String parentWorkflowId);
    }

    final class ParentImpl implements Parent {
        /** IDs of records currently being processed; null until run() initialises it. */
        private Set<String> currentRecords;
        /** Completions that arrive via signal before run() sets currentRecords. */
        private final Set<String> recordsToRemove = new HashSet<>();
        private int totalProcessed = 0;

        @Override
        public void recordCompleted(String recordId) {
            if (currentRecords == null) {
                // Signal arrived before run() started — buffer it.
                recordsToRemove.add(recordId);
                return;
            }
            // Dedupe: remove returns false if the ID was already absent.
            if (currentRecords.remove(recordId)) {
                totalProcessed++;
            }
        }

        @Override
        public int run(Shared.SlidingWindowInput input) {
            this.totalProcessed = input.totalProcessed;
            int windowSize = input.windowSize > 0 ? input.windowSize : Shared.WINDOW_SIZE;
            List<String> recordIds = input.recordIds;
            String parentId = Workflow.getInfo().getWorkflowId();
            int nextIndex = input.startIndex;

            // Restore the in-flight set carried over from the previous run.
            this.currentRecords = new HashSet<>(
                    input.currentRecords != null ? input.currentRecords : Collections.emptySet());
            // Apply any completions that signalled before run() began.
            int earlyCompleted = currentRecords.size();
            currentRecords.removeAll(recordsToRemove);
            this.totalProcessed += earlyCompleted - currentRecords.size();

            // Track start promises so we can wait before continuing-as-new.
            List<Promise<WorkflowExecution>> childrenStarted = new ArrayList<>();

            while (true) {
                // Block until the window has a free slot.
                Workflow.await(() -> currentRecords.size() < windowSize);

                // No more records to launch — drain remaining children and finish.
                if (nextIndex >= recordIds.size()) {
                    Workflow.await(() -> currentRecords.isEmpty());
                    Workflow.getLogger(ParentImpl.class)
                            .info("Sliding window complete: total={} totalProcessed={}",
                                    recordIds.size(), this.totalProcessed);
                    return this.totalProcessed;
                }

                String recordId = recordIds.get(nextIndex);
                ChildWorkflowOptions opts = ChildWorkflowOptions.newBuilder()
                        .setWorkflowId(parentId + "/record-" + recordId)
                        .setTaskQueue(Shared.TASK_QUEUE)
                        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
                        .build();
                Child child = Workflow.newChildWorkflowStub(Child.class, opts);
                Async.procedure(child::run, recordId, parentId);
                // Resolves when the child has actually started (needed before CAN).
                childrenStarted.add(Workflow.getWorkflowExecution(child));
                currentRecords.add(recordId);
                nextIndex++;

                // Continue-as-New after starting windowSize children to keep history short.
                if (childrenStarted.size() >= windowSize) {
                    // Wait for all children to confirm start before handing off.
                    // Without this, CAN could race child startup and they'd never run.
                    Promise.allOf(childrenStarted).get();
                    Workflow.getLogger(ParentImpl.class)
                            .info("ContinueAsNew: nextIndex={} totalProcessed={}", nextIndex, this.totalProcessed);
                    Workflow.newContinueAsNewStub(Parent.class)
                            .run(new Shared.SlidingWindowInput(
                                    recordIds, windowSize, nextIndex, this.totalProcessed, currentRecords));
                    return 0; // unreachable; CAN throws
                }
            }
        }
    }

    final class ChildImpl implements Child {
        private final Activities activities = Workflow.newActivityStub(
                Activities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(30))
                        .build());

        @Override
        public void run(String recordId, String parentWorkflowId) {
            activities.processRecord(recordId);
            Workflow.getLogger(ChildImpl.class).info("Processed record: {}", recordId);

            // Signal the parent that this slot is now free.
            // Ignore if the parent has already completed (final run finished before us).
            ExternalWorkflowStub parent = Workflow.newUntypedExternalWorkflowStub(parentWorkflowId);
            try {
                parent.signal(Shared.COMPLETION_SIGNAL, recordId);
            } catch (Exception e) {
                String msg = e.getMessage() != null ? e.getMessage() : "";
                if (msg.contains("workflow not found") || msg.contains("not found")) {
                    Workflow.getLogger(ChildImpl.class)
                            .info("Parent already completed, signal not needed: {}", recordId);
                } else {
                    throw e;
                }
            }
        }
    }
}
