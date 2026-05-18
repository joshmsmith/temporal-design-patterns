import io.temporal.activity.ActivityInterface;

@ActivityInterface
public interface Activities {
    String processPayment(Shared.PaymentPayload payload);

    String sendWebhookCallback(Shared.CallbackInput input);

    final class Impl implements Activities {
        @Override
        public String processPayment(Shared.PaymentPayload payload) {
            System.out.printf("Processing payment %s for $%.2f%n", payload.paymentId(), payload.amount());
            return String.format("Payment %s processed successfully for $%.2f",
                    payload.paymentId(), payload.amount());
        }

        @Override
        public String sendWebhookCallback(Shared.CallbackInput input) {
            // In production this would POST to input.callbackUrl() with input.payload() as the body.
            // For this demo we skip the actual HTTP call.
            System.out.printf("[stub] Would POST to %s with payload: %s%n",
                    input.callbackUrl(), input.payload());
            return String.format("Callback to %s delivered (stubbed)", input.callbackUrl());
        }
    }
}
