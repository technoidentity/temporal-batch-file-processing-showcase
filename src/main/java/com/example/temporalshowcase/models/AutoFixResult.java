package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AutoFixResult {
    private boolean success;
    private String strategyApplied;
    private String errorMessage;

    public static AutoFixResult success(String strategy) {
        return AutoFixResult.builder().success(true).strategyApplied(strategy).build();
    }

    public static AutoFixResult failure(String error) {
        return AutoFixResult.builder().success(false).errorMessage(error).build();
    }
}
