package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CheckpointData {
    private String filePath;
    private long bytesTransferred;
    private long totalBytes;
    private int segmentCount;
    private String partialChecksum;
    private long savedAt;
}
