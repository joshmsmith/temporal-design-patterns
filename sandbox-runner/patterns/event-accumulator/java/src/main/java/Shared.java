import java.time.Duration;
import java.util.List;

public final class Shared {
    public static final String TASK_QUEUE = "accumulator-task-queue";
    public static final String WORKFLOW_ID_PREFIX = "accumulator";

    // Sliding inactivity window. Use minutes (e.g. Duration.ofMinutes(30)) in production.
    public static final Duration MAX_AWAIT_TIME = Duration.ofSeconds(10);

    public static class OrderItem {
        public String orderId;
        public String itemId;
        public String name;

        public OrderItem() {}

        public OrderItem(String orderId, String itemId, String name) {
            this.orderId = orderId;
            this.itemId = itemId;
            this.name = name;
        }

        @Override
        public String toString() {
            return name + " (" + itemId + ")";
        }
    }

    private Shared() {}
}
