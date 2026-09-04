package com.example.temporalshowcase.workflows.pattern12;

import com.example.temporalshowcase.models.FileMetadata;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.util.List;

/**
 * Pattern 12: File Ordering Dependencies — Dependency Graph
 *
 * Solves: Files with processing order dependencies causing incorrect results.
 * Temporal features: Topological sort via activities, cycle detection and resolution,
 *                    child workflows per file, @QueryMethod for progress, Workflow.sleep()
 *                    while waiting for upstream files to complete.
 */
@WorkflowInterface
public interface DependencyGraphWorkflow {

    @WorkflowMethod(name = "P12-DependencyGraph")
    void manageFileDependencies(List<FileMetadata> files);

    /** Returns the number of files that have been successfully processed. */
    @QueryMethod
    int getCompletedFileCount();

    /** Returns the current processing stage — e.g. "PARSING_HEADERS", "TOPOLOGICAL_SORT", "PROCESSING", "DONE". */
    @QueryMethod
    String getProcessingStage();
}
