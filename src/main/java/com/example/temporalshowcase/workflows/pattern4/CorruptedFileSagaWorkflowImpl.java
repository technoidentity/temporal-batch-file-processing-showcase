package com.example.temporalshowcase.workflows.pattern4;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.FileManagementActivities;
import com.example.temporalshowcase.activities.SagaActivities;
import com.example.temporalshowcase.models.*;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Workflow;

import java.util.ArrayList;
import java.util.List;

/**
 * Pattern 4: Saga Recovery for corrupted files.
 *
 * Flow:
 *   File Validation (Checksum & Integrity)
 *     ├─ File Valid   → Normal Processing Workflow
 *     └─ File Corrupted → Saga Recovery Process:
 *           1. Isolate Corrupted File → quarantine
 *           2. Alert Operations / Notify stakeholders
 *           3. Attempt File Repair
 *                ├─ Repair Success → 4. Resume Processing with fixed file
 *                └─ Repair Failed  → 5. Request Resend
 *                                  → 6. Compensate Actions (LIFO rollback):
 *                                         - Notify Consumers Of Failure
 *                                         - Cancel Downstream Processing
 *                                         - Rollback Database Transactions
 *                                         - Restore Previous File State
 *                                  → 7. Update Status — Mark as resolved
 */
public class CorruptedFileSagaWorkflowImpl implements CorruptedFileSagaWorkflow {

    private final SagaActivities sagaActivities = Workflow.newActivityStub(
            SagaActivities.class,
            TemporalRuntime.activity("saga"));

    private final FileManagementActivities fileActivities = Workflow.newActivityStub(
            FileManagementActivities.class,
            TemporalRuntime.activity("file-ops"));

    // Tracked for @QueryMethod
    private String sagaStatus = "STARTING";

    // ──────────────────────────────────────────────────────────────────────────

    @Override
    public ProcessingResult handleCorruptedFile(FileMetadata fileMetadata, CorruptionDetails corruptionDetails) {

        // ── File Validation: Checksum & Integrity ─────────────────────────────
        sagaStatus = "VALIDATING";
        FileValidationResult validation = fileActivities.validateFile(fileMetadata);

        if (!validation.isCorrupted()) {
            sagaStatus = "NORMAL_PROCESSING";
            ProcessingResult result = fileActivities.processFileContent(fileMetadata);
            sagaStatus = "COMPLETED";
            return result;
        }

        // ── File Corrupted → Saga Recovery Process ────────────────────────────
        List<Compensation> compensations = new ArrayList<>();

        try {
            sagaStatus = "ISOLATING";
            sagaActivities.isolateCorruptedFile(fileMetadata);
            // Register compensations in forward order (executed LIFO on failure)
            compensations.add(new Compensation("restoreFileState",        fileMetadata, "Restore original file state"));
            compensations.add(new Compensation("rollbackDatabaseTransactions", fileMetadata, "Rollback DB transactions"));
            compensations.add(new Compensation("cancelDownstreamProcessing",   fileMetadata, "Cancel downstream jobs"));

            sagaStatus = "ALERTING";
            sagaActivities.alertOperationsTeam(fileMetadata, corruptionDetails);

            sagaStatus = "REPAIRING";
            RepairResult repairResult = sagaActivities.attemptFileRepair(fileMetadata, corruptionDetails);

            if (repairResult.isSuccess()) {
                sagaStatus = "RESUMING";
                ProcessingResult result = sagaActivities.resumeProcessingAfterRepair(repairResult.getRepairedFile());
                sagaStatus = "COMPLETED";
                return result;

            } else {
                sagaStatus = "REQUESTING_RESEND";
                sagaActivities.requestFileResend(fileMetadata);

                sagaStatus = "COMPENSATING";
                executeCompensations(compensations, fileMetadata);

                sagaStatus = "RESOLVED";
                sagaActivities.updateProcessingStatus(fileMetadata,
                        "RESOLVED", "Repair failed — resend requested; compensations complete");

                return ProcessingResult.builder()
                        .status(ProcessingStatus.FAILED)
                        .message("File repair failed; resend requested; compensations executed; marked as resolved")
                        .build();
            }

        } catch (Exception e) {
            // Unexpected failure — execute all registered compensations in reverse
            sagaStatus = "COMPENSATING";
            executeCompensations(compensations, fileMetadata);

            sagaStatus = "RESOLVED";
            sagaActivities.updateProcessingStatus(fileMetadata,
                    "RESOLVED", "Saga failed unexpectedly: " + e.getMessage());

            throw ApplicationFailure.newFailure(
                    "Saga failed and compensations executed: " + e.getMessage(), "SAGA_FAILURE");
        }
    }

    /**
     * Executes compensating actions in LIFO order (Saga rollback).
     * Also calls notifyConsumersOfFailure as part of the compensation process.
     * Individual compensation failures are logged but do not abort the remaining compensations.
     */
    private void executeCompensations(List<Compensation> compensations, FileMetadata fileMetadata) {
        // Notify Consumers Of Failure — always first in compensation sequence
        try {
            sagaActivities.notifyConsumersOfFailure(fileMetadata,
                    "File processing failed — compensations are being executed");
        } catch (Exception ex) {
            Workflow.getLogger(this.getClass()).warn(
                    "notifyConsumersOfFailure failed: {}", ex.getMessage());
        }

        // Remaining compensations in LIFO order
        for (int i = compensations.size() - 1; i >= 0; i--) {
            Compensation comp = compensations.get(i);
            try {
                switch (comp.getActivityName()) {
                    case "restoreFileState" ->
                            sagaActivities.restoreFileState((FileMetadata) comp.getData());
                    case "rollbackDatabaseTransactions" ->
                            sagaActivities.rollbackDatabaseTransactions((FileMetadata) comp.getData());
                    case "cancelDownstreamProcessing" ->
                            sagaActivities.cancelDownstreamProcessing((FileMetadata) comp.getData());
                }
                sagaActivities.logCompensationExecution(comp, "SUCCESS");
            } catch (Exception ex) {
                sagaActivities.logCompensationExecution(comp, "FAILED: " + ex.getMessage());
            }
        }
    }

    @Override
    public String getSagaStatus() {
        return sagaStatus;
    }
}
