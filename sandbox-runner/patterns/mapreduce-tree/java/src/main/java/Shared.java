import java.util.ArrayList;
import java.util.List;

public final class Shared {
    public static final String TASK_QUEUE = "mapreduce-tree-task-queue";
    public static final String WORKFLOW_ID_PREFIX = "mapreduce-tree";
    public static final int LEAF_THRESHOLD = 3;
    public static final int MAX_DEPTH = 5;
    public static final String RESULT_SIGNAL = "nodeResult";

    public static final List<String> RECORDS;

    static {
        RECORDS = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            RECORDS.add("item-" + i);
        }
    }

    private Shared() {}
}
