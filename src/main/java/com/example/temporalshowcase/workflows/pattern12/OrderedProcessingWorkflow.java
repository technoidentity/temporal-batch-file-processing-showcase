package com.example.temporalshowcase.workflows.pattern12;

import com.example.temporalshowcase.models.FileMetadata;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.util.List;

/**
 * Pattern 12 – Ordered Processing Workflow (Dependencies Resolved path).
 *
 * Processes files in topological order through the four-stage pipeline:
 *  Ready Queue → Processing Queue → Completed Queue / Failed Queue
 */
@WorkflowInterface
public interface OrderedProcessingWorkflow {

    @WorkflowMethod(name = "P12-OrderedProcessing")
    void processInOrder(List<FileMetadata> orderedFiles);

    /** Returns pipeline summary — e.g. "ready=2 processing=1 completed=3 failed=0" */
    @QueryMethod
    String getPipelineStatus();
}
