package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChunkResult {
    private int chunkIndex;
    private long recordCount;
    private boolean success;
    private String checksum;
    private List<String> validationResults;
    private String processingMetrics;
}
