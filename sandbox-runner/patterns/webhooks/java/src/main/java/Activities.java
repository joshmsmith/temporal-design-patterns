import io.temporal.activity.ActivityInterface;

@ActivityInterface
public interface Activities {
    String processPayment(Shared.PaymentPayload payload);

    final class Impl implements Activities {
        @Override
        public String processPayment(Shared.PaymentPayload payload) {
            System.out.printf("Processing payment %s for $%.2f%n", payload.paymentId(), payload.amount());
            return String.format("Payment %s processed successfully for $%.2f",
                    payload.paymentId(), payload.amount());
        }
    }
}
