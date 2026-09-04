package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessingState {
    private String fileHash;
    private ProcessingStatus status;
    private long timestamp;
    private ProcessingResult result;
    private String errorMessage;

    public ProcessingState(String fileHash, ProcessingStatus status, long timestamp) {
        this.fileHash = fileHash;
        this.status = status;
        this.timestamp = timestamp;
    }

    public ProcessingState(String fileHash, ProcessingStatus status, ProcessingResult result) {
        this.fileHash = fileHash;
        this.status = status;
        this.result = result;
    }

    public ProcessingState(String fileHash, ProcessingStatus status, String errorMessage) {
        this.fileHash = fileHash;
        this.status = status;
        this.errorMessage = errorMessage;
    }
}
