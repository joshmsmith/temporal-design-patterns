import io.temporal.activity.ActivityInterface;

import java.util.ArrayList;
import java.util.List;

@ActivityInterface
public interface Activities {
    List<Integer> fetchPage(int offset, int pageSize);
    void processRecord(int recordId);

    final class Impl implements Activities {
        @Override
        public List<Integer> fetchPage(int offset, int pageSize) {
            int end = Math.min(offset + pageSize, Shared.TOTAL_RECORDS);
            List<Integer> page = new ArrayList<>();
            for (int i = offset; i < end; i++) {
                page.add(i);
            }
            return page;
        }

        @Override
        public void processRecord(int recordId) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
