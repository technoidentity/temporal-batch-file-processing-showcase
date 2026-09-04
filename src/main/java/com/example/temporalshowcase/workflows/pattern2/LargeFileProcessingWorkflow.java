package com.example.temporalshowcase.workflows.pattern2;

import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.ProcessingResult;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 2: Large File Chunked Processing (Parent Workflow)
 *
 * Solves: Files exceeding memory capacity causing processing failures.
 * Temporal features: Child workflows, parallel Promise execution,
 *                    configurable chunk size, @QueryMethod for live progress.
 */
@WorkflowInterface
public interface LargeFileProcessingWorkflow {

    @WorkflowMethod(name = "P02-LargeFileChunking")
    ProcessingResult processLargeFile(FileMetadata fileMetadata);

    /** Returns live chunk progress — e.g. "PROCESSING_CHUNKS — 3/8 chunks completed" */
    @QueryMethod
    String getProgress();
}
