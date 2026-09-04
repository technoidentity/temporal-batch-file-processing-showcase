package com.example.temporalshowcase.workflows.pattern6;

import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.StandardizedData;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 6: Format Mismatches — Multi-Format Handler
 *
 * Solves: Files in unexpected formats causing parsing failures.
 * Temporal features: Runtime format detection, child workflow routing per format,
 *                    graceful escalation for unknown formats.
 */
@WorkflowInterface
public interface MultiFormatHandlerWorkflow {

    @WorkflowMethod(name = "P06-MultiFormatRouter")
    StandardizedData handleMultiFormatFile(FileMetadata fileMetadata);

    /** Returns the format currently being processed (e.g. CSV, XML, JSON, FIXED_WIDTH, UNKNOWN). */
    @QueryMethod
    String getCurrentFormat();
}
