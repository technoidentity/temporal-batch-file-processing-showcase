package com.example.temporalshowcase.workflows.pattern2;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.FileManagementActivities;
import com.example.temporalshowcase.models.*;
import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Pattern 2: Large File — Parent workflow implementation.
 *
 * Steps:
 *  1. Activity: getFileStats      — determine size, compute chunk strategy (3 attempts, 5min)
 *  2. Fan out child workflows     — parallel via Async.function() + Promise.allOf()
 *  3. Activity: aggregateChunkResults — merge & validate (2 attempts, 30min timeout)
 *
 * Adds @QueryMethod getProgress() so the Temporal UI / monitoring dashboard can
 * show live chunk completion progress.
 */
public class LargeFileProcessingWorkflowImpl implements LargeFileProcessingWorkflow {

    private final long MAX_MEMORY_PER_CHUNK = TemporalRuntime.patterns().getLargeFile().getMaxChunk().toBytes();
    private final long MIN_CHUNK_SIZE       = TemporalRuntime.patterns().getLargeFile().getMinChunk().toBytes();

    // ── Activity stub: getFileStats (Step 1) ─────────────────────────────────
    private final FileManagementActivities statsActivities = Workflow.newActivityStub(
            FileManagementActivities.class,
            TemporalRuntime.activity("file-stats"));

    // ── Activity stub: aggregateChunkResults (Step 3) ────────────────────────
    // 2 attempts, 30 min start-to-close, 1 min initial, 5 min max
    private final FileManagementActivities aggregateActivities = Workflow.newActivityStub(
            FileManagementActivities.class,
            TemporalRuntime.activity("aggregate"));

    // ── Progress state (readable via @QueryMethod) ───────────────────────────
    private int totalChunks     = 0;
    private int completedChunks = 0;
    private String processingStatus = "INITIALIZING";

    @Override
    public ProcessingResult processLargeFile(FileMetadata fileMetadata) {
        processingStatus = "GETTING_FILE_STATS";

        FileStats fileStats = statsActivities.getFileStats(fileMetadata);

        long chunkSize   = calculateOptimalChunkSize(fileStats.getFileSize());
        totalChunks      = (int) Math.ceil((double) fileStats.getFileSize() / chunkSize);
        processingStatus = "SPAWNING_CHILD_WORKFLOWS";

        List<Promise<ChunkResult>> promises = new ArrayList<>();
        for (int i = 0; i < totalChunks; i++) {
            ChunkMetadata chunk = ChunkMetadata.builder()
                    .filePath(fileMetadata.getFilePath())
                    .chunkIndex(i)
                    .chunkOffset((long) i * chunkSize)
                    .chunkSize(chunkSize)
                    .fileChecksum(fileStats.getFileChecksum())
                    .totalChunks(totalChunks)
                    .build();

            ProcessChunkWorkflow childStub = Workflow.newChildWorkflowStub(
                    ProcessChunkWorkflow.class,
                    ChildWorkflowOptions.newBuilder()
                            .setTaskQueue(TemporalRuntime.queue("chunk-processing"))
                            .setWorkflowId("p02-chunk-" + Workflow.getInfo().getWorkflowId() + "-" + chunk.getChunkIndex())
                            // ABANDON: if parent is cancelled/fails, chunks continue independently
                            .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
                            .setWorkflowExecutionTimeout(Duration.ofHours(2))
                            .setRetryOptions(RetryOptions.newBuilder()
                                    .setMaximumAttempts(3)
                                    .setInitialInterval(Duration.ofMinutes(1))
                                    .setMaximumInterval(Duration.ofMinutes(10))
                                    .build())
                            .build());

            promises.add(Async.function(childStub::processChunk, chunk));
        }

        processingStatus = "PROCESSING_CHUNKS";

        Promise.allOf(promises).get();

        List<ChunkResult> results = new ArrayList<>();
        for (Promise<ChunkResult> p : promises) {
            results.add(p.get());
            completedChunks++;
        }

        processingStatus = "AGGREGATING_RESULTS";

        ProcessingResult result = aggregateActivities.aggregateChunkResults(fileMetadata, results);
        processingStatus = "COMPLETED";
        return result;
    }

    @Override
    public String getProgress() {
        if (totalChunks == 0) return processingStatus;
        return String.format("%s — %d/%d chunks completed", processingStatus, completedChunks, totalChunks);
    }

    /**
     * Targets 1/8 of the file per chunk while staying within [MIN_CHUNK_SIZE, MAX_MEMORY_PER_CHUNK].
     * Small files are sent as a single chunk to avoid unnecessary child-workflow overhead.
     */
    private long calculateOptimalChunkSize(long fileSize) {
        if (fileSize <= MAX_MEMORY_PER_CHUNK) return fileSize;
        long optimal = Math.min(fileSize / 8, MAX_MEMORY_PER_CHUNK);
        return Math.max(optimal, MIN_CHUNK_SIZE);
    }
}
