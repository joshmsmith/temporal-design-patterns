import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Accumulator workflow: collects order items sent via signals, deduplicates
 * them, and processes them together after a sliding inactivity timeout.
 *
 * The workflow ID must be derived deterministically from the bucket key so
 * that Signal-With-Start routes all signals for the same order to the same
 * running instance.
 */
@WorkflowInterface
public interface AccumulatorWorkflow {

    @WorkflowMethod
    String accumulate(String bucketKey, List<Shared.OrderItem> items, List<String> seenKeys);

    @SignalMethod
    void addItem(Shared.OrderItem item);

    @SignalMethod
    void exit();

    class Impl implements AccumulatorWorkflow {

        private final Activities activities = Workflow.newActivityStub(
                Activities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Shared.MAX_AWAIT_TIME.plusSeconds(10))
                        .build());

        // Buffer for signals received before the main loop drains them
        private final ArrayDeque<Shared.OrderItem> unprocessed = new ArrayDeque<>();
        private boolean exitRequested = false;

        @Override
        public String accumulate(String bucketKey, List<Shared.OrderItem> itemsInput, List<String> seenKeysInput) {
            // Restore state carried forward from a previous Continue-as-New run
            List<Shared.OrderItem> items = new ArrayList<>(itemsInput);
            Set<String> seenSet = new HashSet<>(seenKeysInput);

            do {
                // Sliding window: wait for a signal or let the inactivity timer fire
                boolean timedOut = !Workflow.await(
                        Shared.MAX_AWAIT_TIME,
                        () -> !unprocessed.isEmpty() || exitRequested);

                // Drain and deduplicate the signal queue
                while (!unprocessed.isEmpty()) {
                    Shared.OrderItem item = unprocessed.removeFirst();
                    if (item.orderId.equals(bucketKey) && seenSet.add(item.itemId)) {
                        items.add(item);
                    }
                }

                if (timedOut || exitRequested) {
                    String result = activities.processItems(bucketKey, items);
                    Workflow.getLogger(Impl.class).info(
                            "Processed batch for " + bucketKey + " with " + items.size() + " items");
                    if (unprocessed.isEmpty()) {
                        return result;
                    }
                    // More signals arrived after timeout/exit — loop to process them
                }
            } while (!unprocessed.isEmpty() || !Workflow.getInfo().isContinueAsNewSuggested());

            // History growing large — continue as new, carrying accumulated state forward
            Workflow.getLogger(Impl.class).info("Continuing as new for " + bucketKey);
            AccumulatorWorkflow continueAsNew = Workflow.newContinueAsNewStub(AccumulatorWorkflow.class);
            continueAsNew.accumulate(bucketKey, items, new ArrayList<>(seenSet));
            return ""; // unreachable — newContinueAsNewStub throws ContinueAsNewException
        }

        @Override
        public void addItem(Shared.OrderItem item) {
            // Signal handlers just buffer — no activity calls inside handlers
            unprocessed.add(item);
        }

        @Override
        public void exit() {
            exitRequested = true;
        }
    }
}
