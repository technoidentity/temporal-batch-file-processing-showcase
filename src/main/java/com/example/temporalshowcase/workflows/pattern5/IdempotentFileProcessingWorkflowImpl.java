package com.example.temporalshowcase.workflows.pattern5;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.FileManagementActivities;
import com.example.temporalshowcase.activities.IdempotencyActivities;
import com.example.temporalshowcase.models.*;
import com.example.temporalshowcase.workflows.pattern1.FileProcessingWorkflow;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Pattern 5: Idempotency implementation.
 *
 * Uses SHA-256 file hash as the idempotency key.  On duplicate delivery
 * the workflow short-circuits and returns the previously stored result.
 * Concurrent duplicates poll with Workflow.sleep() until the in-flight
 * instance completes.
 */
public class IdempotentFileProcessingWorkflowImpl implements IdempotentFileProcessingWorkflow {

    private final Duration WAIT_POLL_INTERVAL = TemporalRuntime.patterns().getIdempotency().getWaitPollInterval();
    private final Duration IN_PROGRESS_TIMEOUT = TemporalRuntime.patterns().getIdempotency().getInProgressTimeout();

    private final FileManagementActivities fileActivities = Workflow.newActivityStub(
            FileManagementActivities.class,
            TemporalRuntime.activity("file-stats"));

    private final IdempotencyActivities idempotencyActivities = Workflow.newActivityStub(
            IdempotencyActivities.class,
            TemporalRuntime.activity("quick"));

    // In-workflow query cache (workflow-local, survives replays)
    private final Map<String, String> deduplicationCache = new HashMap<>();

    @Override
    public ProcessingResult processFileWithIdempotency(FileMetadata fileMetadata) {
        String fileHash = fileActivities.generateFileHash(fileMetadata);
        deduplicationCache.put(fileHash, "CHECKING");

        ProcessingState existingState = idempotencyActivities.checkProcessingState(fileHash);

        if (existingState != null) {
            switch (existingState.getStatus()) {
                case COMPLETED:
                    idempotencyActivities.logDuplicateDetected(fileHash, existingState);
                    idempotencyActivities.logAuditTrail(fileHash, "DUPLICATE_SKIPPED",
                            "Returning previous result — status=COMPLETED");
                    deduplicationCache.put(fileHash, "DUPLICATE_RETURNED");
                    return existingState.getResult();

                case IN_PROGRESS:
                    idempotencyActivities.logAuditTrail(fileHash, "DUPLICATE_WAITING",
                            "Concurrent execution in progress — polling for result");
                    ProcessingResult awaited = waitForInProgressCompletion(fileHash);
                    deduplicationCache.put(fileHash, "AWAITED_RESULT");
                    return awaited;

                default:
                    idempotencyActivities.logAuditTrail(fileHash, "REPROCESSING",
                            "Previous attempt status=" + existingState.getStatus() + " — reprocessing");
                    break;
            }
        }

        // Atomically claim the idempotency key. If a concurrent duplicate won the
        // race, fall back to awaiting its result instead of double-processing.
        boolean claimed = idempotencyActivities.tryBeginProcessing(
                new ProcessingState(fileHash, ProcessingStatus.IN_PROGRESS, Workflow.currentTimeMillis()));
        if (!claimed) {
            idempotencyActivities.logAuditTrail(fileHash, "DUPLICATE_RACE",
                    "Concurrent start detected via CAS — awaiting in-flight result");
            ProcessingResult awaited = waitForInProgressCompletion(fileHash);
            deduplicationCache.put(fileHash, "AWAITED_RESULT");
            return awaited;
        }
        idempotencyActivities.logAuditTrail(fileHash, "PROCESSING_STARTED",
                "Marked in-progress — file=" + fileMetadata.getFileName());
        deduplicationCache.put(fileHash, "IN_PROGRESS");

        try {
            FileProcessingWorkflow child = Workflow.newChildWorkflowStub(
                    FileProcessingWorkflow.class,
                    ChildWorkflowOptions.newBuilder()
                            .setTaskQueue(TemporalRuntime.queue("idempotency"))
                            .setWorkflowId("p05-fileproc-" + Workflow.getInfo().getWorkflowId())
                            .setWorkflowExecutionTimeout(Duration.ofHours(2))
                            .build());

            Promise<ProcessingResult> resultPromise = Async.function(child::processFile, fileMetadata);
            child.fileArrived(fileMetadata);
            ProcessingResult result = resultPromise.get();

            idempotencyActivities.updateProcessingState(
                    new ProcessingState(fileHash, ProcessingStatus.COMPLETED, result));
            idempotencyActivities.logAuditTrail(fileHash, "PROCESSING_COMPLETED",
                    "records=" + result.getRecordCount() + " status=" + result.getStatus());
            deduplicationCache.put(fileHash, "COMPLETED");
            return result;

        } catch (Exception e) {
            idempotencyActivities.updateProcessingState(
                    new ProcessingState(fileHash, ProcessingStatus.FAILED, e.getMessage()));
            idempotencyActivities.logAuditTrail(fileHash, "PROCESSING_FAILED", e.getMessage());
            deduplicationCache.put(fileHash, "FAILED");
            throw e;
        }
    }

    /**
     * Polls the persistent state store until the in-progress execution completes or fails.
     * Uses Workflow.sleep() so the poll is durable and survives worker restarts.
     * Throws if the concurrent execution failed or if IN_PROGRESS_TIMEOUT elapses.
     */
    private ProcessingResult waitForInProgressCompletion(String fileHash) {
        long start   = Workflow.currentTimeMillis();
        long timeout = IN_PROGRESS_TIMEOUT.toMillis();

        while (Workflow.currentTimeMillis() - start < timeout) {
            ProcessingState state = idempotencyActivities.checkProcessingState(fileHash);
            if (state != null) {
                if (state.getStatus() == ProcessingStatus.COMPLETED) return state.getResult();
                if (state.getStatus() == ProcessingStatus.FAILED) {
                    throw ApplicationFailure.newFailure(
                            "Concurrent processing failed: " + state.getErrorMessage(),
                            "CONCURRENT_PROCESSING_FAILED");
                }
            }
            Workflow.sleep(WAIT_POLL_INTERVAL);
        }
        throw ApplicationFailure.newFailure("Timeout waiting for in-progress processing", "WAIT_TIMEOUT");
    }

    @Override
    public String getDeduplicationState(String fileHash) {
        return deduplicationCache.getOrDefault(fileHash, "UNKNOWN");
    }
}
