public final class Shared {
    public static final String TASK_QUEUE = "webhooks-task-queue";
    public static final String WORKFLOW_ID_PREFIX = "order";

    public record OrderInput(String orderId, double amount) {}

    public record PaymentPayload(String paymentId, double amount) {}

    private Shared() {}
}
