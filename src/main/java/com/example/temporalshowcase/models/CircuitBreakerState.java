package com.example.temporalshowcase.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CircuitBreakerState {
    private String key;
    private CircuitState state;
    private int failureCount;
    private int emptyFileCount;
    private long lastFailureTime;
    /** Count of files checked in the current sliding window (max 10). */
    private int windowTotal;
    /** Count of empty files in the current sliding window. */
    private int windowEmpty;
    /** Last 10 observations; {@code true} = empty/failure. */
    @Builder.Default
    private List<Boolean> observations = new ArrayList<>();

    /** Record one file in the sliding window (size from config) and refresh counts. */
    public void recordObservation(boolean empty, int windowSize) {
        if (observations == null) observations = new ArrayList<>();
        observations.add(empty);
        while (observations.size() > windowSize) {
            observations.remove(0);
        }
        windowTotal = observations.size();
        windowEmpty = 0;
        for (Boolean o : observations) {
            if (Boolean.TRUE.equals(o)) windowEmpty++;
        }
        if (empty) emptyFileCount++;
    }

    @JsonIgnore
    public boolean isOpen() { return state == CircuitState.OPEN; }

    @JsonIgnore
    public boolean isHalfOpen() { return state == CircuitState.HALF_OPEN; }

    /** Failure rate in the current sliding window (0.0 – 1.0). */
    @JsonIgnore
    public double getFailureRate() {
        return windowTotal == 0 ? 0.0 : (double) windowEmpty / windowTotal;
    }

    /** True when the window has at least {@code minSamples} AND the rate exceeds {@code threshold}. */
    @JsonIgnore
    public boolean isHighFailureRate(int minSamples, double threshold) {
        return windowTotal >= minSamples && getFailureRate() > threshold;
    }
}
