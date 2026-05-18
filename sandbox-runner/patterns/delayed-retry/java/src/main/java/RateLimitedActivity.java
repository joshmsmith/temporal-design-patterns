import io.temporal.activity.Activity;
import io.temporal.activity.ActivityInterface;
import io.temporal.failure.ApplicationFailure;

import java.time.Duration;

@ActivityInterface
public interface RateLimitedActivity {
    String callApi(String endpoint);

    final class Impl implements RateLimitedActivity {

        @Override
        public String callApi(String endpoint) {
            int attempt = Activity.getExecutionContext().getInfo().getAttempt();
            System.out.printf("Calling API for %s (attempt %d)%n", endpoint, attempt);

            // Simulate an HTTP 429 on the first attempt with a Retry-After: 5s header.
            // The nextRetryDelay overrides the RetryPolicy interval for this single retry.
            if (attempt == 1) {
                int retryAfterSeconds = 5;
                System.out.printf(
                        "  Attempt %d: received HTTP 429 — Retry-After: %ds%n",
                        attempt, retryAfterSeconds);
                System.out.printf(
                        "  Throwing ApplicationFailure with nextRetryDelay=%ds%n",
                        retryAfterSeconds);
                System.out.println(
                        "  (RetryPolicy initialInterval=1s is overridden for this retry only)");
                throw ApplicationFailure.newFailureWithCauseAndDelay(
                        "Rate limited — Retry-After: " + retryAfterSeconds + "s",
                        "RateLimitError",
                        null,
                        Duration.ofSeconds(retryAfterSeconds));
            }

            System.out.printf("  Attempt %d: API call succeeded%n", attempt);
            return "{\"status\": \"ok\", \"data\": \"result-from-" + endpoint + "\"}";
        }
    }
}
