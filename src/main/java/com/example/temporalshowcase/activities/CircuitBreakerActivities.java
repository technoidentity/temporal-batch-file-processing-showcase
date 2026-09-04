package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface CircuitBreakerActivities {

    @ActivityMethod
    CircuitBreakerState getCircuitBreakerState(String circuitBreakerKey);

    @ActivityMethod
    void updateCircuitBreakerState(CircuitBreakerUpdate update);

    /** Records an empty-file observation in the sliding window (last 10 files). */
    @ActivityMethod
    void updateEmptyFileMetrics(String circuitBreakerKey);

    /** Records a non-empty (successful) file so the last-10 failure rate stays honest. */
    @ActivityMethod
    void recordSuccessfulFile(String circuitBreakerKey);

    @ActivityMethod
    void recordCircuitBreakerEvent(String key, String event, String details);
}
