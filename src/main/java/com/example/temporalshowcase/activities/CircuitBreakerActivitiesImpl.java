package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.config.TemporalRuntime;
import com.example.temporalshowcase.models.*;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class CircuitBreakerActivitiesImpl implements CircuitBreakerActivities {

    private static final String STATE_FILE = "circuit-breaker.json";

    private static ConcurrentHashMap<String, CircuitBreakerState> store = new ConcurrentHashMap<>();

    @Value("${app.data.dir:}")
    private String dataDir;

    private ConcurrentHashMap<String, CircuitBreakerState> store() {
        if (store.isEmpty() && JsonFileStore.persistEnabled(dataDir)) {
            store = JsonFileStore.load(dataDir, STATE_FILE, new TypeReference<>() {});
        }
        return store;
    }

    private void persist() {
        JsonFileStore.save(dataDir, STATE_FILE, store);
    }

    @Override
    public CircuitBreakerState getCircuitBreakerState(String circuitBreakerKey) {
        log.debug("Getting circuit breaker state for key: {}", circuitBreakerKey);
        return store().computeIfAbsent(circuitBreakerKey, k ->
                CircuitBreakerState.builder()
                        .key(k)
                        .state(CircuitState.CLOSED)
                        .failureCount(0)
                        .emptyFileCount(0)
                        .lastFailureTime(0)
                        .build());
    }

    @Override
    public void updateCircuitBreakerState(CircuitBreakerUpdate update) {
        log.info("Updating circuit breaker [{}] to state: {}", update.getKey(), update.getNewState());
        store().compute(update.getKey(), (k, existing) -> {
            if (existing == null) {
                existing = CircuitBreakerState.builder().key(k).state(CircuitState.CLOSED).build();
            }
            existing.setState(update.getNewState());
            if (update.getNewState() == CircuitState.OPEN) {
                existing.setLastFailureTime(System.currentTimeMillis());
                existing.setFailureCount(existing.getFailureCount() + 1);
            }
            return existing;
        });
        persist();
    }

    @Override
    public void updateEmptyFileMetrics(String circuitBreakerKey) {
        recordObservation(circuitBreakerKey, true);
    }

    @Override
    public void recordSuccessfulFile(String circuitBreakerKey) {
        recordObservation(circuitBreakerKey, false);
    }

    private void recordObservation(String circuitBreakerKey, boolean empty) {
        store().compute(circuitBreakerKey, (k, state) -> {
            if (state == null) {
                state = CircuitBreakerState.builder().key(k).state(CircuitState.CLOSED).build();
            }
            state.recordObservation(empty, TemporalRuntime.patterns().getCircuitBreaker().getWindowSize());
            log.info("Sliding window [{}/{}] empty rate={}% for key={} empty={}",
                    state.getWindowEmpty(), state.getWindowTotal(),
                    String.format("%.0f", state.getFailureRate() * 100), k, empty);
            return state;
        });
        persist();
    }

    @Override
    public void recordCircuitBreakerEvent(String key, String event, String details) {
        log.info("Circuit breaker event [{}] key={} details={}", event, key, details);
    }
}
