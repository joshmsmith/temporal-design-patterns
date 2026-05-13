import io.temporal.activity.ActivityInterface;

@ActivityInterface
public interface Activities {
    void processRecord(int recordId);

    final class Impl implements Activities {
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
