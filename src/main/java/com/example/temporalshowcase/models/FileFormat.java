package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileFormat {
    private FileFormatType type;
    private String encoding;
    private String delimiter;
    private String schemaVersion;
    private String detectionConfidence;
}
