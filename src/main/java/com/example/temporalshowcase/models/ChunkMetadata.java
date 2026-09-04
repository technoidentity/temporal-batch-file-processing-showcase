package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChunkMetadata {
    private String filePath;
    private int chunkIndex;
    private long chunkSize;
    private long chunkOffset;
    private String fileChecksum;
    private int totalChunks;
}
