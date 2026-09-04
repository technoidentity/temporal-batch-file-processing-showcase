package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class IdempotencyActivitiesImpl implements IdempotencyActivities {

    private static final String STATE_FILE = "idempotency.json";

    private static ConcurrentHashMap<String, ProcessingState> stateStore = new ConcurrentHashMap<>();

    @Value("${app.data.dir:}")
    private String dataDir;

    private ConcurrentHashMap<String, ProcessingState> store() {
        if (stateStore.isEmpty() && JsonFileStore.persistEnabled(dataDir)) {
            stateStore = JsonFileStore.load(dataDir, STATE_FILE, new TypeReference<>() {});
        }
        return stateStore;
    }

    private void persist() {
        JsonFileStore.save(dataDir, STATE_FILE, stateStore);
    }

    @Override
    public ProcessingState checkProcessingState(String fileHash) {
        log.debug("Checking processing state for hash: {}", fileHash);
        return store().get(fileHash);
    }

    @Override
    public void recordProcessingStart(ProcessingState state) {
        log.debug("Recording processing start for hash: {}", state.getFileHash());
        store().put(state.getFileHash(), state);
        persist();
    }

    @Override
    public boolean tryBeginProcessing(ProcessingState state) {
        boolean won = store().putIfAbsent(state.getFileHash(), state) == null;
        log.debug("tryBeginProcessing hash={} won={}", state.getFileHash(), won);
        if (won) persist();
        return won;
    }

    @Override
    public void updateProcessingState(ProcessingState state) {
        log.debug("Updating processing state for hash: {} to: {}", state.getFileHash(), state.getStatus());
        store().put(state.getFileHash(), state);
        persist();
    }

    @Override
    public void logDuplicateDetected(String fileHash, ProcessingState existingState) {
        log.warn("AUDIT — DUPLICATE_DETECTED hash={} previous_status={}", fileHash, existingState.getStatus());
    }

    @Override
    public void logAuditTrail(String fileHash, String event, String details) {
        log.info("AUDIT — event={} hash={} details={}", event, fileHash, details);
    }
}
