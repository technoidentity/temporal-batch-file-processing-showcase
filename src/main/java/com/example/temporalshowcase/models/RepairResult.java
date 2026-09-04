package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RepairResult {
    private boolean success;
    private FileMetadata repairedFile;
    private String repairStrategy;
    private String errorMessage;

    public static RepairResult success(FileMetadata repairedFile, String strategy) {
        return RepairResult.builder()
                .success(true)
                .repairedFile(repairedFile)
                .repairStrategy(strategy)
                .build();
    }

    public static RepairResult failure(String error) {
        return RepairResult.builder()
                .success(false)
                .errorMessage(error)
                .build();
    }
}
