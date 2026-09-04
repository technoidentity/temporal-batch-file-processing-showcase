package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferState {
    private TransferRequest request;
    private long bytesTransferred;
    private long totalBytes;
    private int segmentCount;
    private String partialChecksum;
    private boolean complete;
    private long lastCheckpointTime;
    /** Tracks how many times the transfer has been restarted from the beginning (Step 7). */
    private int restartCount;

    public TransferState(TransferRequest request) {
        this.request = request;
        this.bytesTransferred = 0;
        this.totalBytes = request.getExpectedFileSize();
        this.complete = false;
    }

    public void updateProgress(long bytes, String checksum) {
        this.bytesTransferred += bytes;
        this.partialChecksum = checksum;
        this.segmentCount++;
        // Unknown size (expectedFileSize=0) is complete only when the copier reports no remaining bytes.
        if (this.totalBytes > 0 && this.bytesTransferred >= this.totalBytes) {
            this.complete = true;
        } else if (this.totalBytes <= 0 && bytes == 0 && "complete".equals(checksum)) {
            this.complete = true;
        }
    }

    /** Rollback to last checkpoint position - stores checkpoint offset for reload. */
    private long checkpointOffset = 0;

    public void rollbackToCheckpoint() {
        // Restore to last saved checkpoint position
        this.bytesTransferred = this.checkpointOffset;
        this.complete = false;
    }

    public void saveCheckpointOffset() {
        this.checkpointOffset = this.bytesTransferred;
    }

    public void restart() {
        this.bytesTransferred = 0;
        this.segmentCount = 0;
        this.partialChecksum = null;
        this.complete = false;
        this.restartCount++;
    }

    /** Step 4 – restore transfer state from a saved checkpoint so we continue from the right offset. */
    public void resumeFromCheckpoint(CheckpointData checkpoint) {
        this.bytesTransferred   = checkpoint.getBytesTransferred();
        this.checkpointOffset   = checkpoint.getBytesTransferred();
        this.totalBytes         = checkpoint.getTotalBytes() > 0 ? checkpoint.getTotalBytes() : this.totalBytes;
        this.segmentCount       = checkpoint.getSegmentCount();
        this.partialChecksum    = checkpoint.getPartialChecksum();
        this.lastCheckpointTime = checkpoint.getSavedAt();
        this.complete = this.totalBytes > 0 && this.bytesTransferred >= this.totalBytes;
    }
}
