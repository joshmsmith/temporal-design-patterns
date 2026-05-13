import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class Shared {
    public static final String TASK_QUEUE = "sliding-window-task-queue";
    public static final String WORKFLOW_ID_PREFIX = "sliding-window";
    public static final int WINDOW_SIZE = 3;
    public static final String COMPLETION_SIGNAL = "recordCompleted";

    public static final List<String> RECORD_IDS;

    static {
        RECORD_IDS = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            RECORD_IDS.add("record-" + i);
        }
    }

    private Shared() {}

    /** Input for the parent sliding-window workflow. Bundled as one object so
     *  every Continue-as-New call has a consistent argument shape. */
    public static final class SlidingWindowInput {
        public List<String> recordIds;
        public int windowSize;
        public int startIndex;
        public int totalProcessed;
        /** IDs of child workflows still in-flight from the previous run. */
        public Set<String> currentRecords;

        public SlidingWindowInput() {}

        public SlidingWindowInput(List<String> recordIds, int windowSize,
                                   int startIndex, int totalProcessed, Set<String> currentRecords) {
            this.recordIds = recordIds;
            this.windowSize = windowSize;
            this.startIndex = startIndex;
            this.totalProcessed = totalProcessed;
            this.currentRecords = currentRecords;
        }
    }
}
