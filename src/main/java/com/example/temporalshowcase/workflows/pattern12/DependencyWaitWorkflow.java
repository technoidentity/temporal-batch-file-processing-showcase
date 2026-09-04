package com.example.temporalshowcase.workflows.pattern12;

import com.example.temporalshowcase.models.FileMetadata;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.util.List;

/**
 * Pattern 12 – Dependency Wait Workflow (Dependencies Missing path).
 *
 * Polls until all declared dependencies are satisfied, then signals
 * the parent workflow that this file is ready for processing.
 */
@WorkflowInterface
public interface DependencyWaitWorkflow {

    @WorkflowMethod(name = "P12-DependencyWait")
    void waitUntilDependenciesMet(FileMetadata file, List<String> missingDeps);

    /** Returns wait state — e.g. "WAITING missing=[fileA.csv]" or "READY". */
    @QueryMethod
    String getWaitStatus();
}
