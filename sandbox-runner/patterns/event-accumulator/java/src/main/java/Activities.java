import io.temporal.activity.ActivityInterface;

import java.util.List;
import java.util.stream.Collectors;

@ActivityInterface
public interface Activities {
    String processItems(String orderId, List<Shared.OrderItem> items);

    class Impl implements Activities {
        @Override
        public String processItems(String orderId, List<Shared.OrderItem> items) {
            String names = items.stream()
                    .map(i -> i.name)
                    .collect(Collectors.joining(", "));
            return "Order " + orderId + " fulfilled with " + items.size() + " item(s): " + names;
        }
    }
}
