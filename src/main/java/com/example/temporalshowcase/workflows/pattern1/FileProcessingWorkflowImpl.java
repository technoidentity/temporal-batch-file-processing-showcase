package com.example.temporalshowcase.workflows.pattern1;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.FileManagementActivities;
import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.ProcessingResult;
import com.example.temporalshowcase.models.ProcessingStatus;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.Workflow;

import java.time.Duration;

/**
 * Pattern 1: Signal-based File Arrival Detection
 *
 * The workflow starts and waits for a `fileArrived` signal carrying FileMetadata.
 * If no signal arrives within 24 hours the SLA deadline is breached and an
 * ApplicationFailure is thrown so Temporal records the violation durably.
 */
public class FileProcessingWorkflowImpl implements FileProcessingWorkflow {

    private final FileManagementActivities fileActivities = Workflow.newActivityStub(
            FileManagementActivities.class,
            TemporalRuntime.activity("file-ops"));

    // Mutable state updated via signal — must only be mutated in signal/workflow thread
    private volatile FileMetadata arrivedFileMetadata;
    private volatile String processingStatus = "WAITING_FOR_FILE";
    private boolean slaExpired = false;

    @Override
    public ProcessingResult processFile(FileMetadata initialMetadata) {
        processingStatus = "WAITING_FOR_FILE_SIGNAL";

        // Caller-supplied SLA if provided, otherwise the configured default (application.yml).
        Duration slaTimeout = (initialMetadata.getSlaTimeoutSeconds() > 0)
                ? Duration.ofSeconds(initialMetadata.getSlaTimeoutSeconds())
                : TemporalRuntime.fileArrivalSla();

        // Run the SLA timer inside a cancellation scope so it can be explicitly
        // cancelled once the file arrives, instead of leaving an outstanding 24h
        // timer on the Temporal service (good practice at scale).
        CancellationScope slaTimerScope = Workflow.newCancellationScope(() ->
                Workflow.newTimer(slaTimeout).thenApply(v -> {
                    slaExpired = true;
                    return null;
                }));
        slaTimerScope.run();

        // Block until the fileArrived signal delivers metadata or the SLA expires.
        Workflow.await(() -> arrivedFileMetadata != null || slaExpired);

        if (arrivedFileMetadata == null) {
            processingStatus = "SLA_VIOLATED";

            // Alert the monitoring system / SLA dashboard before recording the failure
            ProcessingResult slaViolation = ProcessingResult.builder()
                    .status(ProcessingStatus.FAILED)
                    .recordCount(0)
                    .message("SLA_TIMEOUT — file did not arrive within " + slaTimeout)
                    .build();
            fileActivities.notifyProcessingComplete(initialMetadata, slaViolation);

            throw ApplicationFailure.newFailure(
                    "File arrival timeout - SLA of " + slaTimeout + " breached for: " + initialMetadata.getFileName(),
                    "SLA_TIMEOUT");
        }

        // File arrived — cancel the still-pending SLA timer.
        slaTimerScope.cancel();
        processingStatus = "PROCESSING";

        var validation = fileActivities.validateFile(arrivedFileMetadata);
        if (validation.isEmpty()) {
            processingStatus = "SKIPPED_EMPTY";
            fileActivities.logEmptyFile(arrivedFileMetadata);
            return ProcessingResult.builder()
                    .status(ProcessingStatus.SKIPPED)
                    .message("File arrived but was empty")
                    .build();
        }

        // Process the file content (activities carry their own retry policy)
        ProcessingResult result = fileActivities.processFileContent(arrivedFileMetadata);
        fileActivities.notifyProcessingComplete(arrivedFileMetadata, result);

        processingStatus = "COMPLETED";
        return result;
    }

    @Override
    public void fileArrived(FileMetadata fileMetadata) {
        this.arrivedFileMetadata = fileMetadata;
        this.processingStatus = "FILE_SIGNAL_RECEIVED";
    }

    @Override
    public String getProcessingStatus() {
        return processingStatus;
    }
}
