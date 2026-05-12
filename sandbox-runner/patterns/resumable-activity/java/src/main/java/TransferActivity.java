import io.temporal.activity.ActivityInterface;
import io.temporal.failure.ApplicationFailure;

import java.util.Set;

@ActivityInterface
public interface TransferActivity {
    void executeTransfer(Shared.TransferInput transfer);

    final class Impl implements TransferActivity {
        // Valid accounts for the demo — any other account number fails.
        private static final Set<String> VALID_ACCOUNTS = Set.of(
                "account-123", "account-456", "account-789");

        @Override
        public void executeTransfer(Shared.TransferInput transfer) {
            System.out.printf(
                    "Executing transfer: %.2f from %s to %s%n",
                    transfer.amount(), transfer.fromAccount(), transfer.toAccount());

            if (!VALID_ACCOUNTS.contains(transfer.toAccount())) {
                throw ApplicationFailure.newNonRetryableFailure(
                        "Account " + transfer.toAccount() + " not found",
                        "AccountNotFoundError");
            }

            System.out.printf(
                    "Transfer of %.2f to %s completed%n",
                    transfer.amount(), transfer.toAccount());
        }
    }
}
