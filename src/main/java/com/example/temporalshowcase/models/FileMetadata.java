package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class FileMetadata {
    private String filePath;
    private String fileName;
    private long fileSize;
    private String fileChecksum;
    private String fileType;
    private long arrivalTimestamp;
    private String sourceSystem;
    /** Override the 24 h SLA for testing. 0 = use the default 24 h. */
    private long slaTimeoutSeconds;
    /** Lock acquisition priority for Pattern 11. Higher = more important. Default 1. */
    private int priority;
    /** Declared upstream file dependencies for Pattern 12 (file names or paths). */
    private List<String> dependencies;
    /** Pattern 11: after acquiring the lock, hold it this many seconds before processing (0 = none). */
    private int holdLockSeconds;
}
