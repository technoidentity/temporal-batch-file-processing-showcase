package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.util.List;

@ActivityInterface
public interface FileManagementActivities {

    /** Returns file size, SHA-256 checksum, and line count from the filesystem. */
    @ActivityMethod
    FileStats getFileStats(FileMetadata fileMetadata);

    /**
     * Checks file existence, size, and checksum integrity.
     * Returns a result with {@code empty}, {@code valid}, and {@code corrupted} flags.
     */
    @ActivityMethod
    FileValidationResult validateFile(FileMetadata fileMetadata);

    /** Computes and returns the SHA-256 hex digest of the file's content. */
    @ActivityMethod
    String generateFileHash(FileMetadata fileMetadata);

    /** Convenience wrapper: runs downloadChunk then processChunkData in one activity call. */
    @ActivityMethod
    ChunkResult downloadAndProcessChunk(ChunkMetadata chunkMetadata);

    /** Step 1 of child workflow: read raw bytes for one chunk (15 min timeout). */
    @ActivityMethod
    ChunkResult downloadChunk(ChunkMetadata chunkMetadata);

    /** Step 2 of child workflow: parse records from the downloaded chunk (10 min timeout). */
    @ActivityMethod
    ChunkResult processChunkData(ChunkMetadata chunkMetadata);

    @ActivityMethod
    ProcessingResult aggregateChunkResults(FileMetadata fileMetadata, List<ChunkResult> chunkResults);

    @ActivityMethod
    ProcessingResult processFileContent(FileMetadata fileMetadata);

    @ActivityMethod
    void logEmptyFile(FileMetadata fileMetadata);

    @ActivityMethod
    void notifyProcessingComplete(FileMetadata fileMetadata, ProcessingResult result);
}
