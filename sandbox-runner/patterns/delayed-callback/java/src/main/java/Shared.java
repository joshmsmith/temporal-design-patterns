public final class Shared {
    public static final String TASK_QUEUE = "webhooks-task-queue";
    public static final String WORKFLOW_ID_PREFIX = "order";
    public static final String CALLBACK_WORKFLOW_ID_PREFIX = "delayed-callback";

    public record OrderInput(String orderId, double amount) {}

    public record PaymentPayload(String paymentId, double amount) {}

    public record CallbackInput(String callbackUrl, String payload, int delaySeconds) {}

    private Shared() {}
}
