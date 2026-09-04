package com.example.temporalshowcase.workflows.pattern1;

import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.ProcessingResult;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 1: Signal-based File Arrival Detection
 *
 * Solves: Files arriving outside expected time windows causing SLA violations.
 * Temporal features: @SignalMethod, @QueryMethod, Workflow.await() with timeout,
 *                    configurable SLA monitoring, durable execution.
 */
@WorkflowInterface
public interface FileProcessingWorkflow {

    @WorkflowMethod(name = "P01-FileProcessing")
    ProcessingResult processFile(FileMetadata fileMetadata);

    /** Signal sent by the external file monitor when the file arrives. */
    @SignalMethod
    void fileArrived(FileMetadata fileMetadata);

    /** Query used by monitoring dashboards to inspect current processing state. */
    @QueryMethod
    String getProcessingStatus();
}
