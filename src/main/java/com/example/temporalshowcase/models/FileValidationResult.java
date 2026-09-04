package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileValidationResult {
    private boolean empty;
    private boolean valid;
    private boolean corrupted;
    private String fileHash;
    private long fileSize;
    private String message;
}
