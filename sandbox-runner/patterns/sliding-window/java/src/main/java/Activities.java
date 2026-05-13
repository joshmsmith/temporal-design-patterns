import io.temporal.activity.ActivityInterface;

@ActivityInterface
public interface Activities {
    void processRecord(String recordId);

    final class Impl implements Activities {
        @Override
        public void processRecord(String recordId) {
            try {
                // Simulate processing work.
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
