package com.example.temporalshowcase.workflows.pattern8;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.TransferActivities;
import com.example.temporalshowcase.models.*;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Workflow;

import java.time.Duration;

/**
 * Pattern 8: Resumable Transfer (Network Failure path).
 *
 * Recovery flow:
 *  1. Detect Transfer Interruption      — detectTransferInterruption()
 *  2. Save Checkpoint Transfer progress — saveTransferCheckpoint()
 *  3. Wait for Network Recovery         — waitForNetworkRecovery() with latency + bandwidth check
 *  4. Resume Transfer From checkpoint   — loadTransferCheckpoint() restores offset
 *  5. Validate Integrity / Checksum     — validateTransferIntegrity()
 *  6. Success  → Complete Transfer      — workflow exits normally
 *  7. Failure  → Restart from beginning — transferState.restart() + loop again (max 3 times)
 */
public class ResumableTransferWorkflowImpl implements ResumableTransferWorkflow {

    private final Duration NETWORK_RECOVERY_TIMEOUT      = TemporalRuntime.patterns().getTransfer().getNetworkRecoveryTimeout();
    private final Duration BASE_NETWORK_POLL_INTERVAL    = TemporalRuntime.patterns().getTransfer().getBaseNetworkPollInterval();
    private final int      MAX_RESTART_ATTEMPTS          = TemporalRuntime.patterns().getTransfer().getMaxRestartAttempts();
    private final Duration MAX_WORKFLOW_TIMEOUT          = TemporalRuntime.patterns().getTransfer().getMaxWorkflowTimeout();
    private final int      MAX_NETWORK_RECOVERY_ATTEMPTS = TemporalRuntime.patterns().getTransfer().getMaxNetworkRecoveryAttempts();

    private final TransferActivities transferActivities = Workflow.newActivityStub(
            TransferActivities.class,
            TemporalRuntime.activity("transfer"));

    // @QueryMethod state
    private String transferProgress = "STARTING";

    @Override
    public void executeResumableTransfer(TransferRequest transferRequest) {
        long workflowStartTime = Workflow.currentTimeMillis();

        int restartCount = 0;

        // Outer restart loop — handles Step 7 (Restart Transfer From Beginning)
        while (restartCount <= MAX_RESTART_ATTEMPTS) {

            TransferState transferState = new TransferState(transferRequest);
            transferState.setRestartCount(restartCount);

            // ── Step 4 bootstrap: load existing checkpoint to resume, not restart from 0 ──
            if (restartCount == 0) {
                CheckpointData existing = transferActivities.loadTransferCheckpoint(
                        transferRequest.getDestinationPath());
                if (existing != null && existing.getBytesTransferred() > 0) {
                    transferState.resumeFromCheckpoint(existing);
                    transferProgress = "RESUMING_FROM_CHECKPOINT — " + existing.getBytesTransferred() + " bytes already done";
                }
            }

            int networkRecoveryAttempts = 0;

            try {
                while (!transferState.isComplete()) {
                    // Guard: total workflow timeout
                    if (Workflow.currentTimeMillis() - workflowStartTime > MAX_WORKFLOW_TIMEOUT.toMillis()) {
                        throw ApplicationFailure.newFailure(
                                "Transfer exceeded maximum workflow timeout of " + MAX_WORKFLOW_TIMEOUT,
                                "WORKFLOW_TIMEOUT");
                    }

                    // Check network before each segment — uses simulation flags if set
                    NetworkStatus network = transferActivities.checkNetworkConnectivityWithSimulation(transferRequest);

                    if (!network.isConnected()) {
                        networkRecoveryAttempts++;
                        if (networkRecoveryAttempts > MAX_NETWORK_RECOVERY_ATTEMPTS) {
                            throw ApplicationFailure.newFailure(
                                    "Max network recovery attempts (" + MAX_NETWORK_RECOVERY_ATTEMPTS + ") exceeded",
                                    "NETWORK_RECOVERY_FAILED");
                        }

                        // ── Step 1: Detect Transfer Interruption ──
                        transferProgress = "INTERRUPTED (recovery attempt " + networkRecoveryAttempts + ")";
                        transferActivities.detectTransferInterruption(transferState);

                        // ── Step 2: Save Checkpoint ──
                        transferProgress = "SAVING_CHECKPOINT";
                        transferState.saveCheckpointOffset();
                        saveCheckpoint(transferState);

                        // ── Step 3: Wait for Network Recovery ──
                        transferProgress = "WAITING_NETWORK_RECOVERY";
                        waitForNetworkRecoveryWithBackoff(transferRequest, networkRecoveryAttempts);

                        // ── Step 4: Resume Transfer From Checkpoint ──
                        transferProgress = "RESUMING_FROM_CHECKPOINT";
                        CheckpointData cp = transferActivities.loadTransferCheckpoint(
                                transferRequest.getDestinationPath());
                        if (cp != null && cp.getBytesTransferred() > 0) {
                            transferState.resumeFromCheckpoint(cp);
                        }
                        continue;
                    }

                    // Network stable — reset recovery counter
                    networkRecoveryAttempts = 0;

                    transferProgress = "TRANSFERRING — " + transferState.getBytesTransferred()
                            + "/" + transferState.getTotalBytes() + " bytes";
                    TransferResult result = transferActivities.executeTransferSegment(transferState);

                    if (result.isSuccess()) {
                        transferState.updateProgress(result.getBytesTransferred(), result.getChecksum());
                        transferActivities.updateTransferProgress(transferState);
                        transferState.saveCheckpointOffset();
                        saveCheckpoint(transferState);
                    } else if (result.isRetryable()) {
                        transferProgress = "RETRYING_SEGMENT — rolling back to checkpoint";
                        transferState.rollbackToCheckpoint();
                        CheckpointData cp = transferActivities.loadTransferCheckpoint(
                                transferRequest.getDestinationPath());
                        if (cp != null) {
                            transferState.resumeFromCheckpoint(cp);
                        }
                    } else {
                        transferState.restart();
                    }
                }

                // ── Step 5: Validate Integrity / Checksum Verification ──
                transferProgress = "VALIDATING_INTEGRITY";
                boolean valid = transferActivities.validateTransferIntegrity(transferState);

                if (valid) {
                    // ── Step 6: Success → Complete Transfer ──
                    transferProgress = "COMPLETED";
                    return;
                }

                // ── Step 7: Failure → Restart Transfer From Beginning ──
                restartCount++;
                if (restartCount > MAX_RESTART_ATTEMPTS) {
                    transferProgress = "FAILED — max restarts exceeded after " + MAX_RESTART_ATTEMPTS + " attempts";
                    throw ApplicationFailure.newFailure(
                            "Transfer integrity check failed after " + MAX_RESTART_ATTEMPTS + " restarts",
                            "CHECKSUM_MISMATCH");
                }
                transferProgress = "RESTARTING_FROM_BEGINNING (attempt " + restartCount + "/" + MAX_RESTART_ATTEMPTS + ")";

            } catch (ApplicationFailure af) {
                transferProgress = "FAILED — " + af.getMessage();
                throw af;
            } catch (Exception e) {
                transferProgress = "FAILED — " + e.getMessage();
                saveCheckpoint(transferState);
                throw ApplicationFailure.newFailure("Transfer failed: " + e.getMessage(), "TRANSFER_FAILURE");
            }
        }
    }

    /** Persists the current byte offset and checksums so the transfer can be resumed after failure. */
    private void saveCheckpoint(TransferState transferState) {
        CheckpointData checkpoint = CheckpointData.builder()
                .filePath(transferState.getRequest().getDestinationPath())
                .bytesTransferred(transferState.getBytesTransferred())
                .totalBytes(transferState.getTotalBytes())
                .segmentCount(transferState.getSegmentCount())
                .partialChecksum(transferState.getPartialChecksum())
                .savedAt(Workflow.currentTimeMillis())
                .build();
        transferActivities.saveTransferCheckpoint(checkpoint);
    }

    /**
     * Step 3 – Wait for Network Recovery with exponential backoff and jitter.
     * Polls connectivity with increasing intervals and validates bandwidth (≥ 1 Mbps) and
     * latency (≤ 500 ms) before declaring the network stable.
     */
    private void waitForNetworkRecoveryWithBackoff(TransferRequest request, int attempt) {
        long start   = Workflow.currentTimeMillis();
        long timeout = NETWORK_RECOVERY_TIMEOUT.toMillis();
        int pollAttempt = 0;

        while (Workflow.currentTimeMillis() - start < timeout) {
            NetworkStatus status = transferActivities.checkNetworkConnectivityWithSimulation(request);
            boolean lowLatency    = status.getLatencyMs() <= 500;
            boolean goodBandwidth = status.getBandwidthMbps() >= 1.0;
            if (status.isConnected() && status.isStable() && lowLatency && goodBandwidth) {
                return;
            }

            // Exponential backoff with jitter: base * 2^attempt + random jitter
            long backoffMs = calculateBackoffWithJitter(pollAttempt);
            Workflow.sleep(Duration.ofMillis(backoffMs));
            pollAttempt++;
        }
        throw ApplicationFailure.newFailure(
                "Network recovery timed out after " + NETWORK_RECOVERY_TIMEOUT, "NETWORK_TIMEOUT");
    }

    /**
     * Calculate exponential backoff with jitter to avoid thundering herd.
     * Formula: min(maxInterval, baseInterval * 2^attempt) + jitter
     */
    private long calculateBackoffWithJitter(int attempt) {
        long baseMs = BASE_NETWORK_POLL_INTERVAL.toMillis();
        long maxMs = Duration.ofMinutes(5).toMillis();

        // Exponential backoff: base * 2^attempt
        long backoff = (long) (baseMs * Math.pow(2, Math.min(attempt, 4)));
        backoff = Math.min(backoff, maxMs);

        // Add jitter: random value between 0 and 25% of backoff
        long jitter = (long) (Workflow.newRandom().nextDouble() * backoff * 0.25);

        return backoff + jitter;
    }

    @Override
    public String getTransferProgress() {
        return transferProgress;
    }
}
