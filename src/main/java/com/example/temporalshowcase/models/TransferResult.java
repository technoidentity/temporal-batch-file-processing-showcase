package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferResult {
    private boolean success;
    private boolean retryable;
    private long bytesTransferred;
    private String checksum;
    private String errorMessage;

    public static TransferResult success(long bytes, String checksum) {
        return TransferResult.builder().success(true).bytesTransferred(bytes).checksum(checksum).build();
    }

    public static TransferResult failure(String error, boolean retryable) {
        return TransferResult.builder().success(false).retryable(retryable).errorMessage(error).build();
    }
}
