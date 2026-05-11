import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;

@WorkflowInterface
public interface OrderWorkflow {
    @WorkflowMethod
    String run(Shared.OrderInput order);

    @SignalMethod
    void paymentReceived(Shared.PaymentPayload payload);

    final class Impl implements OrderWorkflow {
        private final Activities activities = Workflow.newActivityStub(
                Activities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(30))
                        .build());

        private Shared.PaymentPayload payment = null;

        @Override
        public String run(Shared.OrderInput order) {
            System.out.println("Order " + order.orderId() + ": waiting for payment webhook");

            // Block until the inbound webhook signal arrives (or timeout after 24 hours)
            boolean received = Workflow.await(Duration.ofHours(24), () -> payment != null);
            if (!received) {
                return "Order " + order.orderId() + ": timed out waiting for payment";
            }

            return activities.processPayment(payment);
        }

        @Override
        public void paymentReceived(Shared.PaymentPayload payload) {
            System.out.println("Payment signal received: " + payload.paymentId());
            this.payment = payload;
        }
    }
}
