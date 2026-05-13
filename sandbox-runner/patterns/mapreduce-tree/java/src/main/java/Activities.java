import io.temporal.activity.ActivityInterface;

@ActivityInterface
public interface Activities {
    String processLeaf(String record);

    final class Impl implements Activities {
        @Override
        public String processLeaf(String record) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "processed(" + record + ")";
        }
    }
}
