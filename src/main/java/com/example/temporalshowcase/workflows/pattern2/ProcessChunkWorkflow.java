package com.example.temporalshowcase.workflows.pattern2;

import com.example.temporalshowcase.models.ChunkMetadata;
import com.example.temporalshowcase.models.ChunkResult;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 2: Process Chunk Child Workflow
 *
 * Each chunk is processed by a dedicated child workflow, providing fault isolation:
 * a failure in one chunk does not affect others.
 */
@WorkflowInterface
public interface ProcessChunkWorkflow {

    @WorkflowMethod(name = "P02-ChunkProcessor")
    ChunkResult processChunk(ChunkMetadata chunkMetadata);
}
