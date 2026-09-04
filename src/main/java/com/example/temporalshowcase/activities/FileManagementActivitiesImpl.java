package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

@Slf4j
@Component
public class FileManagementActivitiesImpl implements FileManagementActivities {

    @Value("${app.data.dir:./data}")
    private String dataDir;

    // ──────────────────────────────────────────────────────────────
    // Internal helpers
    // ──────────────────────────────────────────────────────────────

    /** Resolves the file path: absolute paths are used as-is; relative paths
     *  are resolved against app.data.dir so callers can pass bare names. */
    private Path resolve(String filePath) {
        Path p = Paths.get(filePath);
        return p.isAbsolute() ? p : Paths.get(dataDir).resolve(filePath);
    }

    private String sha256(Path path) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (InputStream is = Files.newInputStream(path)) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = is.read(buf)) != -1) md.update(buf, 0, n);
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException | IOException e) {
            log.warn("Hash computation failed for {}: {}", path, e.getMessage());
            return "hash-error";
        }
    }

    private long countLines(Path path) {
        try {
            return Files.lines(path, StandardCharsets.UTF_8).count();
        } catch (IOException e) {
            log.warn("Line count failed for {}: {}", path, e.getMessage());
            return 0;
        }
    }

    /**
     * Emits an activity heartbeat so a stuck worker is detected within the
     * stub's heartbeatTimeout instead of waiting for the full startToClose.
     * Guarded so the same methods can still be called directly from a unit test
     * outside an activity execution context.
     */
    private void heartbeat(Object detail) {
        try {
            io.temporal.activity.Activity.getExecutionContext().heartbeat(detail);
        } catch (Exception ignored) {
            // not running inside an activity worker — safe to skip
        }
    }

    // ──────────────────────────────────────────────────────────────
    // Activity implementations
    // ──────────────────────────────────────────────────────────────

    @Override
    public FileStats getFileStats(FileMetadata fileMetadata) {
        Path path = resolve(fileMetadata.getFilePath());
        log.debug("Getting real file stats for: {}", path);
        try {
            long size = Files.size(path);
            long lines = countLines(path);
            String hash = sha256(path);
            return FileStats.builder()
                    .fileSize(size)
                    .fileChecksum(hash)
                    .lineCount(lines)
                    .encoding("UTF-8")
                    .build();
        } catch (IOException e) {
            log.warn("Could not stat file {}: {}", path, e.getMessage());
            return FileStats.builder()
                    .fileSize(0).fileChecksum("").lineCount(0).encoding("UTF-8").build();
        }
    }

    @Override
    public FileValidationResult validateFile(FileMetadata fileMetadata) {
        Path path = resolve(fileMetadata.getFilePath());
        log.debug("Validating file: {}", path);
        boolean exists = Files.exists(path);
        if (!exists) {
            return FileValidationResult.builder()
                    .empty(true).valid(false).corrupted(false)
                    .fileHash("").fileSize(0)
                    .message("File not found: " + path)
                    .build();
        }
        try {
            long size = Files.size(path);
            boolean empty = size == 0;
            if (empty) {
                return FileValidationResult.builder()
                        .empty(true).valid(false).corrupted(false)
                        .fileHash("").fileSize(0)
                        .message("File exists but is empty (0 bytes)")
                        .build();
            }
            String hash = sha256(path);

            // Checksum integrity check — only compare when a non-empty expected checksum is stored
            // and it is in SHA-256 format (64 hex chars). Skip UUID/empty values.
            boolean corrupted = false;
            String corruptionMessage = null;
            String storedChecksum = fileMetadata.getFileChecksum();
            boolean hasRealChecksum = storedChecksum != null
                    && storedChecksum.length() == 64           // SHA-256 is always 64 hex chars
                    && storedChecksum.matches("[0-9a-fA-F]+"); // only hex digits
            if (hasRealChecksum && !storedChecksum.equalsIgnoreCase(hash)) {
                corrupted = true;
                corruptionMessage = "Checksum mismatch — expected=" + storedChecksum + " actual=" + hash;
            } else {
                String content = java.nio.file.Files.readString(path, java.nio.charset.StandardCharsets.UTF_8);
                if (content.contains("CORRUPT") || content.contains("???INVALID???")) {
                    corrupted = true;
                    corruptionMessage = "Corruption markers detected in file content";
                }
            }

            return FileValidationResult.builder()
                    .empty(false)
                    .valid(!corrupted)
                    .corrupted(corrupted)
                    .fileHash(hash)
                    .fileSize(size)
                    .message(corrupted ? corruptionMessage : "File is valid — " + size + " bytes, checksum OK")
                    .build();
        } catch (IOException e) {
            return FileValidationResult.builder()
                    .empty(false).valid(false).corrupted(false).fileHash("").fileSize(0)
                    .message("IO error reading file: " + e.getMessage())
                    .build();
        }
    }

    @Override
    public String generateFileHash(FileMetadata fileMetadata) {
        Path path = resolve(fileMetadata.getFilePath());
        log.debug("Computing SHA-256 hash for: {}", path);
        if (!Files.exists(path)) {
            log.warn("File not found for hashing: {}", path);
            return "file-not-found";
        }
        return sha256(path);
    }

    @Override
    public ChunkResult downloadAndProcessChunk(ChunkMetadata chunkMetadata) {
        // Convenience wrapper used by tests / controller — delegates to both steps
        ChunkResult downloaded = downloadChunk(chunkMetadata);
        if (!downloaded.isSuccess()) return downloaded;
        return processChunkData(chunkMetadata);
    }

    /** Step 1 — Read raw bytes for the chunk (simulates a remote download). */
    @Override
    public ChunkResult downloadChunk(ChunkMetadata chunkMetadata) {
        Path path = resolve(chunkMetadata.getFilePath());
        log.debug("downloadChunk: chunk={} offset={} size={} file={}",
                chunkMetadata.getChunkIndex(), chunkMetadata.getChunkOffset(),
                chunkMetadata.getChunkSize(), path);
        heartbeat("downloadChunk:start:" + chunkMetadata.getChunkIndex());
        try (SeekableByteChannel ch = Files.newByteChannel(path, StandardOpenOption.READ)) {
            ch.position(chunkMetadata.getChunkOffset());
            int bufSize = (int) Math.min(chunkMetadata.getChunkSize(), ch.size() - ch.position());
            if (bufSize <= 0) {
                return ChunkResult.builder()
                        .chunkIndex(chunkMetadata.getChunkIndex())
                        .recordCount(0).success(true).checksum("empty").build();
            }
            ByteBuffer buf = ByteBuffer.allocate(bufSize);
            int n;
            while (buf.hasRemaining() && (n = ch.read(buf)) != -1) {
                heartbeat("downloadChunk:progress:" + chunkMetadata.getChunkIndex() + ":" + buf.position());
            }
            String chunkHash = HexFormat.of().formatHex(
                    MessageDigest.getInstance("MD5").digest(buf.array()));
            log.debug("downloadChunk complete: chunk={} bytes={} hash={}",
                    chunkMetadata.getChunkIndex(), bufSize, chunkHash);
            return ChunkResult.builder()
                    .chunkIndex(chunkMetadata.getChunkIndex())
                    .recordCount(bufSize)   // bytes read — processChunkData will convert to records
                    .success(true)
                    .checksum(chunkHash)
                    .build();
        } catch (Exception e) {
            log.error("downloadChunk failed for chunk={}: {}", chunkMetadata.getChunkIndex(), e.getMessage());
            return ChunkResult.builder()
                    .chunkIndex(chunkMetadata.getChunkIndex())
                    .recordCount(0).success(false).checksum("error").build();
        }
    }

    /** Step 2 — Parse record count from the chunk bytes (CPU-bound analysis). */
    @Override
    public ChunkResult processChunkData(ChunkMetadata chunkMetadata) {
        Path path = resolve(chunkMetadata.getFilePath());
        log.debug("processChunkData: chunk={} offset={} file={}",
                chunkMetadata.getChunkIndex(), chunkMetadata.getChunkOffset(), path);
        try (SeekableByteChannel ch = Files.newByteChannel(path, StandardOpenOption.READ)) {
            ch.position(chunkMetadata.getChunkOffset());
            int bufSize = (int) Math.min(chunkMetadata.getChunkSize(), ch.size() - ch.position());
            if (bufSize <= 0) {
                return ChunkResult.builder()
                        .chunkIndex(chunkMetadata.getChunkIndex())
                        .recordCount(0).success(true).checksum("empty").build();
            }
            ByteBuffer buf = ByteBuffer.allocate(bufSize);
            int bytesRead = 0, n;
            heartbeat("processChunkData:start:" + chunkMetadata.getChunkIndex());
            while (buf.hasRemaining() && (n = ch.read(buf)) != -1) {
                bytesRead += n;
                heartbeat("processChunkData:progress:" + chunkMetadata.getChunkIndex() + ":" + bytesRead);
            }
            String content = new String(buf.array(), 0, bytesRead, StandardCharsets.UTF_8);
            long lineCount = content.lines().filter(l -> !l.isBlank()).count();
            String hash = HexFormat.of().formatHex(
                    MessageDigest.getInstance("MD5").digest(buf.array()));
            log.info("processChunkData complete: chunk={} records={}", chunkMetadata.getChunkIndex(), lineCount);
            return ChunkResult.builder()
                    .chunkIndex(chunkMetadata.getChunkIndex())
                    .recordCount(lineCount)
                    .success(true)
                    .checksum(hash)
                    .build();
        } catch (Exception e) {
            log.error("processChunkData failed for chunk={}: {}", chunkMetadata.getChunkIndex(), e.getMessage());
            return ChunkResult.builder()
                    .chunkIndex(chunkMetadata.getChunkIndex())
                    .recordCount(0).success(false).checksum("error").build();
        }
    }

    @Override
    public ProcessingResult aggregateChunkResults(FileMetadata fileMetadata, List<ChunkResult> chunkResults) {
        long totalRecords = chunkResults.stream().mapToLong(ChunkResult::getRecordCount).sum();
        long failedChunks = chunkResults.stream().filter(c -> !c.isSuccess()).count();
        // Use the file's actual checksum if available; otherwise combine chunk checksums
        String fileHash = (fileMetadata.getFileChecksum() != null && !fileMetadata.getFileChecksum().isBlank())
                ? fileMetadata.getFileChecksum()
                : chunkResults.stream().map(ChunkResult::getChecksum)
                        .filter(c -> c != null && !c.isBlank() && !"error".equals(c) && !"empty".equals(c))
                        .reduce("", (a, b) -> a + b);
        log.info("Aggregating {} chunks for {}: {} records, {} failures",
                chunkResults.size(), fileMetadata.getFileName(), totalRecords, failedChunks);
        return ProcessingResult.builder()
                .fileHash(fileHash)
                .status(failedChunks == 0 ? ProcessingStatus.COMPLETED : ProcessingStatus.FAILED)
                .recordCount(totalRecords)
                .message(String.format("Processed %d chunks → %d records (%d failed chunks)",
                        chunkResults.size(), totalRecords, failedChunks))
                .build();
    }

    @Override
    public ProcessingResult processFileContent(FileMetadata fileMetadata) {
        Path path = resolve(fileMetadata.getFilePath());
        log.debug("Processing file content: {}", path);
        if (!Files.exists(path)) {
            return ProcessingResult.builder()
                    .fileHash("")
                    .status(ProcessingStatus.FAILED)
                    .message("File not found: " + path)
                    .recordCount(0).build();
        }
        try {
            heartbeat("processFileContent:" + fileMetadata.getFileName());
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            long dataLines = lines.stream().filter(l -> !l.isBlank()).count();
            boolean hasHeader = !lines.isEmpty() && lines.get(0).contains(",");
            long recordCount = hasHeader ? Math.max(0, dataLines - 1) : dataLines;
            String hash = sha256(path);

            log.info("Processed {}: {} data records, hash={}", fileMetadata.getFileName(), recordCount, hash);
            return ProcessingResult.builder()
                    .fileHash(hash)
                    .status(ProcessingStatus.COMPLETED)
                    .recordCount(recordCount)
                    .message(String.format("Processed %s — %d records from %d lines",
                            fileMetadata.getFileName(), recordCount, lines.size()))
                    .build();
        } catch (IOException e) {
            log.error("Failed to process {}: {}", path, e.getMessage());
            return ProcessingResult.builder()
                    .fileHash("")
                    .status(ProcessingStatus.FAILED)
                    .message("IO error: " + e.getMessage())
                    .recordCount(0).build();
        }
    }

    @Override
    public void logEmptyFile(FileMetadata fileMetadata) {
        log.warn("EMPTY FILE DETECTED — path={} source={}", fileMetadata.getFilePath(), fileMetadata.getSourceSystem());
    }

    @Override
    public void notifyProcessingComplete(FileMetadata fileMetadata, ProcessingResult result) {
        log.info("PROCESSING COMPLETE — file={} records={} status={} hash={}",
                fileMetadata.getFileName(), result.getRecordCount(),
                result.getStatus(), result.getFileHash());
    }
}
