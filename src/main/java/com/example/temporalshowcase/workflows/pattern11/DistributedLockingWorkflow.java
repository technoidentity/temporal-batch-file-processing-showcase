package com.example.temporalshowcase.workflows.pattern11;

import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.ProcessingResult;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 11: Concurrent Access — Distributed Locking
 */
@WorkflowInterface
public interface DistributedLockingWorkflow {

    @WorkflowMethod(name = "P11-DistributedFileLock")
    ProcessingResult processFileWithLock(FileMetadata fileMetadata);

    /** Returns current lock state — e.g. "ACQUIRING", "PROCESSING", "RELEASING", "COMPLETED". */
    @QueryMethod
    String getLockStatus();
}
