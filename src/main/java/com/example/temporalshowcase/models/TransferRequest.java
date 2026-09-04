package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferRequest {
    private String sourceUrl;
    private String destinationPath;
    private String fileName;
    private long expectedFileSize;
    private String expectedChecksum;
    private int maxRetries;
    /** When > 0, the first N connectivity checks simulate a network outage. */
    private int simulateNetworkFailureCount;
    /** When true, integrity validation is forced to fail to trigger Step 7 restart. */
    private boolean simulateIntegrityFailure;
}
