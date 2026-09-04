package com.example.temporalshowcase.activities;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tiny JSON persistence for showcase stores (circuit / idempotency / lock).
 * Survives process restart when {@code app.data.dir} is set. Tests leave
 * {@code dataDir} unset so they stay in-memory only.
 */
@Slf4j
final class JsonFileStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonFileStore() {}

    static Path stateFile(String dataDir, String name) {
        return Path.of(dataDir, "state", name);
    }

    static boolean persistEnabled(String dataDir) {
        return dataDir != null && !dataDir.isBlank();
    }

    static <T> ConcurrentHashMap<String, T> load(String dataDir, String name, TypeReference<ConcurrentHashMap<String, T>> type) {
        ConcurrentHashMap<String, T> empty = new ConcurrentHashMap<>();
        if (!persistEnabled(dataDir)) return empty;
        Path file = stateFile(dataDir, name);
        if (!Files.exists(file)) return empty;
        try {
            ConcurrentHashMap<String, T> loaded = MAPPER.readValue(file.toFile(), type);
            return loaded != null ? loaded : empty;
        } catch (IOException e) {
            log.warn("Could not load state {}: {}", file, e.getMessage());
            return empty;
        }
    }

    static void save(String dataDir, String name, Object value) {
        if (!persistEnabled(dataDir)) return;
        Path file = stateFile(dataDir, name);
        try {
            Files.createDirectories(file.getParent());
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), value);
        } catch (IOException e) {
            log.warn("Could not save state {}: {}", file, e.getMessage());
        }
    }
}
