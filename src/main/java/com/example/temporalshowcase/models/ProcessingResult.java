package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessingResult {
    private String fileHash;
    private ProcessingStatus status;
    private long recordCount;
    private String message;
    private long processingDurationMs;
}
