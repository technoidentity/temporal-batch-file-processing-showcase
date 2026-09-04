package com.example.temporalshowcase.workflows.pattern8;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.TransferActivities;
import com.example.temporalshowcase.models.*;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Workflow;


/**
 * Pattern 8 – Normal Transfer (Network OK path).
 * Executes the full transfer in a single shot with standard Temporal retries.
 * No checkpoint management needed — if the transfer fails Temporal retries it.
 */
public class NormalTransferWorkflowImpl implements NormalTransferWorkflow {

    private final TransferActivities transferActivities = Workflow.newActivityStub(
            TransferActivities.class,
            TemporalRuntime.activity("transfer-normal"));

    @Override
    public void executeNormalTransfer(TransferRequest transferRequest) {
        TransferState transferState = new TransferState(transferRequest);

        // Transfer all segments sequentially (no checkpointing needed on stable network)
        while (!transferState.isComplete()) {
            TransferResult result = transferActivities.executeTransferSegment(transferState);
            if (result.isSuccess()) {
                transferState.updateProgress(result.getBytesTransferred(), result.getChecksum());
                transferActivities.updateTransferProgress(transferState);
            } else {
                throw ApplicationFailure.newFailure(
                        "Transfer failed on stable network: " + result.getErrorMessage(),
                        "NORMAL_TRANSFER_FAILURE");
            }
        }

        boolean valid = transferActivities.validateTransferIntegrity(transferState);
        if (!valid) {
            throw ApplicationFailure.newFailure(
                    "Integrity check failed after normal transfer", "CHECKSUM_MISMATCH");
        }
    }
}
