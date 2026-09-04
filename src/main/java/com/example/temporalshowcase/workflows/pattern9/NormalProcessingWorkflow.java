package com.example.temporalshowcase.workflows.pattern9;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 9 – Within Limits branch: direct processing, no throttling needed.
 */
@WorkflowInterface
public interface NormalProcessingWorkflow {

    @WorkflowMethod(name = "P09-NormalProcessing")
    void executeNormalProcessing(int filesToProcess);
}
