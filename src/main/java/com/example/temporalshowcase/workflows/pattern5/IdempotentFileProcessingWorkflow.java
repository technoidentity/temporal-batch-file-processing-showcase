package com.example.temporalshowcase.workflows.pattern5;

import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.ProcessingResult;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 5: Duplicate Files — Idempotency Pattern
 *
 * Solves: Duplicate file deliveries causing data duplication and inconsistent state.
 * Temporal features: Workflow state store, @QueryMethod for dedup state,
 *                    child workflow delegation, Workflow.sleep() for in-progress wait.
 */
@WorkflowInterface
public interface IdempotentFileProcessingWorkflow {

    @WorkflowMethod(name = "P05-DuplicateFileIdempotency")
    ProcessingResult processFileWithIdempotency(FileMetadata fileMetadata);

    /** Query returns current deduplication state for a file hash. */
    @QueryMethod
    String getDeduplicationState(String fileHash);
}
