public final class Shared {
    public static final String TASK_QUEUE = "resumable-activity-task-queue";
    public static final String WORKFLOW_ID_PREFIX = "resumable-activity";

    public record TransferInput(String fromAccount, String toAccount, double amount) {}

    private Shared() {}
}
