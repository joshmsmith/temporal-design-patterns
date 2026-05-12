import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;

@WorkflowInterface
public interface TransferWorkflow {
    @WorkflowMethod
    String run(Shared.TransferInput transfer);

    @SignalMethod
    void retryWithCorrection(String correctedAccount);

    @SignalMethod
    void approve(boolean approved);

    @QueryMethod
    String getStatus();

    final class Impl implements TransferWorkflow {
        private String status = "PENDING";
        private String correctedAccount;
        private Boolean approval;

        private final TransferActivity activity = Workflow.newActivityStub(
                TransferActivity.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(10))
                        .setRetryOptions(RetryOptions.newBuilder()
                                .setMaximumAttempts(1)
                                .build())
                        .build());

        @Override
        public String run(Shared.TransferInput transfer) {
            String account = transfer.toAccount();
            int correctionCount = 0;

            while (true) {
                status = "TRANSFERRING";
                try {
                    activity.executeTransfer(
                            new Shared.TransferInput(transfer.fromAccount(), account, transfer.amount()));
                    break; // Activity succeeded — exit the correction loop
                } catch (ActivityFailure e) {
                    correctionCount++;
                    if (correctionCount > 5) {
                        status = "FAILED";
                        throw e;
                    }
                    status = "AWAITING_CORRECTION";
                    Workflow.getLogger(TransferWorkflow.class).warn(
                            "Transfer failed — waiting for account correction: {}", account);
                    // Park until the admin sends a correction signal
                    Workflow.await(() -> correctedAccount != null);
                    account = correctedAccount;
                    correctedAccount = null;
                }
            }

            status = "AWAITING_APPROVAL";
            Workflow.await(() -> approval != null);

            if (approval) {
                status = "COMPLETED";
                return String.format("Transfer of %.2f to %s completed", transfer.amount(), account);
            }
            status = "REJECTED";
            return "Transfer rejected by client";
        }

        @Override
        public void retryWithCorrection(String account) {
            this.correctedAccount = account;
        }

        @Override
        public void approve(boolean decision) {
            this.approval = decision;
        }

        @Override
        public String getStatus() {
            return status;
        }
    }
}
