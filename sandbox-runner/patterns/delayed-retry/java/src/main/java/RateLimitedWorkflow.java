import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;

@WorkflowInterface
public interface RateLimitedWorkflow {
    @WorkflowMethod
    String run(String endpoint);

    final class Impl implements RateLimitedWorkflow {

        // Normal RetryPolicy: 1s initial interval.
        // The nextRetryDelay set in the activity overrides this for the rate-limited attempt.
        private final RateLimitedActivity activity = Workflow.newActivityStub(
                RateLimitedActivity.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(10))
                        .setRetryOptions(RetryOptions.newBuilder()
                                .setInitialInterval(Duration.ofSeconds(1))
                                .setBackoffCoefficient(2.0)
                                .setMaximumAttempts(5)
                                .build())
                        .build());

        @Override
        public String run(String endpoint) {
            Workflow.getLogger(RateLimitedWorkflow.class)
                    .info("Calling rate-limited API: {}", endpoint);
            String result = activity.callApi(endpoint);
            Workflow.getLogger(RateLimitedWorkflow.class)
                    .info("API call succeeded: {}", result);
            return result;
        }
    }
}
