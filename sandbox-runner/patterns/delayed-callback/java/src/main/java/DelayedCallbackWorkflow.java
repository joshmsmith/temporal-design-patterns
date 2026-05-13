import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;

@WorkflowInterface
public interface DelayedCallbackWorkflow {
    @WorkflowMethod
    String run(Shared.CallbackInput input);

    final class Impl implements DelayedCallbackWorkflow {
        private final Activities activities = Workflow.newActivityStub(
                Activities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofMinutes(5))
                        .build());

        @Override
        public String run(Shared.CallbackInput input) {
            System.out.println("Sleeping " + input.delaySeconds() + "s before calling " + input.callbackUrl());

            // Durable sleep — survives worker restarts, server restarts, everything
            Workflow.sleep(Duration.ofSeconds(input.delaySeconds()));

            // Fire the outbound callback; Temporal retries on HTTP failure
            String result = activities.sendWebhookCallback(input);
            System.out.println("Callback delivered to " + input.callbackUrl());
            return result;
        }
    }
}
