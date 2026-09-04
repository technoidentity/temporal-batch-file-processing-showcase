package com.example.temporalshowcase.workflows.pattern4;

import com.example.temporalshowcase.models.CorruptionDetails;
import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.ProcessingResult;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 4: Corrupted Files — Saga Recovery
 *
 * Solves: Corrupted or partially transferred files causing data integrity issues.
 * Temporal features: Saga pattern with explicit compensation list, reverse compensation
 *                    on failure, durable recovery steps, query for live status.
 */
@WorkflowInterface
public interface CorruptedFileSagaWorkflow {

    @WorkflowMethod(name = "P04-CorruptedFileSaga")
    ProcessingResult handleCorruptedFile(FileMetadata fileMetadata, CorruptionDetails corruptionDetails);

    /** Returns the current saga step — e.g. VALIDATING, ISOLATING, REPAIRING, COMPENSATING, RESOLVED. */
    @QueryMethod
    String getSagaStatus();
}
