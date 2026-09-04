package com.example.temporalshowcase.workflows.pattern2;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.FileManagementActivities;
import com.example.temporalshowcase.models.ChunkMetadata;
import com.example.temporalshowcase.models.ChunkResult;
import io.temporal.workflow.Workflow;


/**
 * Pattern 2: Process Chunk Child Workflow.
 *
 * Step 1 — downloadChunk    : 3 attempts, 15 min start-to-close
 * Step 2 — processChunkData : 3 attempts, 10 min start-to-close
 * Failed downloads are retried on CHUNK_RETRY_QUEUE.
 */
public class ProcessChunkWorkflowImpl implements ProcessChunkWorkflow {

    private final FileManagementActivities downloadActivities = Workflow.newActivityStub(
            FileManagementActivities.class,
            TemporalRuntime.activity("chunk-download"));

    private final FileManagementActivities retryDownloadActivities = Workflow.newActivityStub(
            FileManagementActivities.class,
            TemporalRuntime.activityOnQueue("chunk-download", "chunk-retry"));

    private final FileManagementActivities processActivities = Workflow.newActivityStub(
            FileManagementActivities.class,
            TemporalRuntime.activity("chunk-process"));

    @Override
    public ChunkResult processChunk(ChunkMetadata chunkMetadata) {
        ChunkResult downloaded = downloadActivities.downloadChunk(chunkMetadata);
        if (!downloaded.isSuccess()) {
            downloaded = retryDownloadActivities.downloadChunk(chunkMetadata);
            if (!downloaded.isSuccess()) {
                return downloaded;
            }
        }
        return processActivities.processChunkData(chunkMetadata);
    }
}
