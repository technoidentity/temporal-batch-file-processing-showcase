package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StandardizedData {
    private String sourceFile;
    private FileFormatType originalFormat;
    private long recordCount;
    private Map<String, Object> data;
    private String standardizedFormat;
}
