package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.config.TemporalRuntime;
import com.example.temporalshowcase.models.*;
import com.example.temporalshowcase.sftp.SftpGateway;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransferActivitiesImpl implements TransferActivities {

    /** Segment size read from config (lazily, so bean construction order is irrelevant). */
    private long segmentBytes() {
        return TemporalRuntime.patterns().getTransfer().getSegment().toBytes();
    }

    private final ObjectMapper objectMapper;

    @Value("${app.data.dir:./data}")
    private String dataDir;

    private Path resolve(String filePath) {
        Path p = Paths.get(filePath);
        return p.isAbsolute() ? p : Paths.get(dataDir).resolve(filePath);
    }

    private Path outgoingDir() {
        return Paths.get(dataDir, "outgoing");
    }

    private Path checkpointDir() {
        return Paths.get(dataDir, "checkpoints");
    }

    /** Returns the checkpoint file path for a given source file path. */
    private Path checkpointFile(String filePath) {
        String safeName = Paths.get(filePath).getFileName().toString().replace("/", "_") + ".checkpoint.json";
        return checkpointDir().resolve(safeName);
    }

    // ──────────────────────────────────────────────────────────────

    /** Per-workflow network-failure simulation counter (keyed by fileName). */
    private static final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicInteger>
            networkFailCounters = new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public NetworkStatus checkNetworkConnectivity() {
        // Simulate connectivity check by verifying the outgoing directory is writable
        boolean writable = Files.isWritable(Paths.get(dataDir));
        log.debug("Network/storage connectivity check: writable={}", writable);
        return NetworkStatus.builder()
                .connected(writable)
                .stable(writable)
                .latencyMs(writable ? 5 : 9999)
                .bandwidthMbps(writable ? 1000.0 : 0.0)
                .build();
    }

    /**
     * Connectivity check that honours simulateNetworkFailureCount from the request.
     * Used by ResumableTransferWorkflowImpl so the first N checks return DISCONNECTED.
     */
    @Override
    public NetworkStatus checkNetworkConnectivityWithSimulation(TransferRequest req) {
        if (req.getSimulateNetworkFailureCount() > 0) {
            java.util.concurrent.atomic.AtomicInteger counter =
                    networkFailCounters.computeIfAbsent(req.getFileName(),
                            k -> new java.util.concurrent.atomic.AtomicInteger(req.getSimulateNetworkFailureCount()));
            if (counter.getAndDecrement() > 0) {
                log.info("SIMULATED network failure for {} — {} failure checks remaining",
                        req.getFileName(), counter.get());
                return NetworkStatus.builder().connected(false).stable(false)
                        .latencyMs(9999).bandwidthMbps(0).build();
            }
            networkFailCounters.remove(req.getFileName());
        }
        return checkNetworkConnectivity();
    }

    /** Transfers the next segmentBytes(). Real SFTP GET when SFTP is enabled, else local copy. */
    @Override
    public TransferResult executeTransferSegment(TransferState transferState) {
        if (TemporalRuntime.sftp().isEnabled()) {
            return executeSftpSegment(transferState);
        }
        Path source = resolve(transferState.getRequest().getDestinationPath());
        String destName = transferState.getRequest().getFileName();
        Path dest;
        try {
            Files.createDirectories(outgoingDir());
            dest = outgoingDir().resolve(destName);
        } catch (IOException e) {
            log.error("Cannot create outgoing dir: {}", e.getMessage());
            return TransferResult.failure("Cannot create outgoing directory: " + e.getMessage(), true);
        }

        long offset = transferState.getBytesTransferred();
        log.debug("Transfer segment — file={} offset={} segmentSize={}",
                source.getFileName(), offset, segmentBytes());

        if (offset == 0) {
            try {
                Files.deleteIfExists(dest);
            } catch (IOException e) {
                log.warn("Could not clear leftover dest {}: {}", dest, e.getMessage());
            }
        }

        try (SeekableByteChannel src = Files.newByteChannel(source, StandardOpenOption.READ);
             FileChannel dst = FileChannel.open(dest,
                     StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

            long srcSize = src.size();
            if (offset >= srcSize) {
                return TransferResult.success(0, "complete");
            }
            src.position(offset);
            long remaining = Math.min(segmentBytes(), srcSize - offset);
            ByteBuffer buf = ByteBuffer.allocate((int) remaining);
            int bytesRead = 0, n;
            while (buf.hasRemaining() && (n = src.read(buf)) != -1) bytesRead += n;
            buf.flip();
            dst.position(offset);
            dst.write(buf);

            // Calculate checksum for this segment
            buf.rewind();
            String segmentChecksum = calculateChecksum(buf);

            log.debug("Transferred {} bytes at offset {} for {} — checksum={}",
                    bytesRead, offset, destName, segmentChecksum);
            return TransferResult.success(bytesRead, segmentChecksum);
        } catch (IOException e) {
            log.error("Transfer segment failed for {}: {}", source, e.getMessage());
            return TransferResult.failure("IO error: " + e.getMessage(), true);
        }
    }

    /** Real SFTP byte-range GET into data/outgoing, resuming from the checkpoint offset. */
    private TransferResult executeSftpSegment(TransferState state) {
        String remoteName = state.getRequest().getFileName();
        long offset = state.getBytesTransferred();
        Path dest;
        try {
            Files.createDirectories(outgoingDir());
            dest = outgoingDir().resolve(remoteName);
            if (offset == 0) {
                Files.deleteIfExists(dest);
            }
        } catch (IOException e) {
            return TransferResult.failure("Cannot prepare outgoing dir: " + e.getMessage(), true);
        }
        try {
            SftpGateway gateway = new SftpGateway(TemporalRuntime.sftp());
            int n = gateway.downloadSegment(remoteName, offset, (int) segmentBytes(), dest);
            if (n <= 0) {
                log.info("SFTP transfer complete for {} at {} bytes", remoteName, offset);
                return TransferResult.success(0, "complete");
            }
            log.info("SFTP segment GET {} offset={} bytes={} -> outgoing/{}", remoteName, offset, n, remoteName);
            return TransferResult.success(n, String.format("crc32-seg-%d", offset));
        } catch (Exception e) {
            log.warn("SFTP segment failed for {} at offset {}: {}", remoteName, offset, e.getMessage());
            return TransferResult.failure("SFTP error: " + e.getMessage(), true);
        }
    }

    /** Calculate simple checksum for a byte buffer (CRC32). */
    private String calculateChecksum(ByteBuffer buffer) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(buffer);
        return String.format("crc32-%08x", crc.getValue());
    }

    @Override
    public void saveTransferCheckpoint(CheckpointData checkpointData) {
        try {
            Files.createDirectories(checkpointDir());
            Path cpFile = checkpointFile(checkpointData.getFilePath());
            objectMapper.writeValue(cpFile.toFile(), checkpointData);
            log.debug("Checkpoint saved: {} ({}/{} bytes)",
                    cpFile.getFileName(), checkpointData.getBytesTransferred(), checkpointData.getTotalBytes());
        } catch (IOException e) {
            log.error("Failed to save checkpoint for {}: {}", checkpointData.getFilePath(), e.getMessage());
        }
    }

    @Override
    public CheckpointData loadTransferCheckpoint(String filePath) {
        Path cpFile = checkpointFile(filePath);
        if (Files.exists(cpFile)) {
            try {
                CheckpointData data = objectMapper.readValue(cpFile.toFile(), CheckpointData.class);
                log.info("Checkpoint loaded for {}: {} bytes already transferred",
                        filePath, data.getBytesTransferred());
                return data;
            } catch (IOException e) {
                log.warn("Corrupt checkpoint for {}, starting fresh: {}", filePath, e.getMessage());
            }
        }
        log.debug("No checkpoint found for {}, starting from 0", filePath);
        return CheckpointData.builder()
                .filePath(filePath)
                .bytesTransferred(0)
                .totalBytes(0)
                .savedAt(System.currentTimeMillis())
                .build();
    }

    @Override
    public boolean validateTransferIntegrity(TransferState transferState) {
        // Honour simulation flag for Scenario 3 (integrity failure → restart from beginning)
        if (transferState.getRequest().isSimulateIntegrityFailure()
                && transferState.getRestartCount() == 0) {
            log.warn("SIMULATED integrity failure for {} — will trigger Step 7 restart",
                    transferState.getRequest().getFileName());
            return false;
        }
        if (TemporalRuntime.sftp().isEnabled()) {
            return validateSftpIntegrity(transferState);
        }
        Path source = resolve(transferState.getRequest().getDestinationPath());
        Path dest = outgoingDir().resolve(transferState.getRequest().getFileName());
        try {
            boolean sourceExists = Files.exists(source);
            boolean destExists = Files.exists(dest);
            if (!sourceExists || !destExists) {
                log.warn("Integrity check FAILED — source={} dest={}", sourceExists, destExists);
                return false;
            }
            long srcSize = Files.size(source);
            long dstSize = Files.size(dest);

            if (srcSize != dstSize) {
                log.warn("Integrity check FAILED — size mismatch: srcSize={} dstSize={}", srcSize, dstSize);
                return false;
            }

            // Validate full file checksum
            String srcChecksum = calculateFileChecksum(source);
            String dstChecksum = calculateFileChecksum(dest);
            boolean checksumMatch = srcChecksum.equals(dstChecksum);

            log.info("Transfer integrity check — srcSize={} dstSize={} srcChecksum={} dstChecksum={} match={}",
                    srcSize, dstSize, srcChecksum, dstChecksum, checksumMatch);
            return checksumMatch;
        } catch (IOException e) {
            log.error("Integrity check error: {}", e.getMessage());
            return false;
        }
    }

    /** SFTP integrity: downloaded file size must equal the remote file size (SFTP stat). */
    private boolean validateSftpIntegrity(TransferState state) {
        String remoteName = state.getRequest().getFileName();
        Path dest = outgoingDir().resolve(remoteName);
        try {
            if (!Files.exists(dest)) {
                log.warn("SFTP integrity FAILED — dest missing for {}", remoteName);
                return false;
            }
            long remoteSize = new SftpGateway(TemporalRuntime.sftp()).size(remoteName);
            long destSize = Files.size(dest);
            boolean ok = remoteSize == destSize;
            log.info("SFTP integrity — remoteSize={} destSize={} match={}", remoteSize, destSize, ok);
            return ok;
        } catch (Exception e) {
            log.error("SFTP integrity error for {}: {}", remoteName, e.getMessage());
            return false;
        }
    }

    /** Calculate CRC32 checksum for an entire file. */
    private String calculateFileChecksum(Path filePath) throws IOException {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        try (java.io.InputStream in = Files.newInputStream(filePath)) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = in.read(buffer)) != -1) {
                crc.update(buffer, 0, len);
            }
        }
        return String.format("crc32-%08x", crc.getValue());
    }

    @Override
    public void updateTransferProgress(TransferState transferState) {
        long pct = transferState.getTotalBytes() > 0
                ? (transferState.getBytesTransferred() * 100 / transferState.getTotalBytes())
                : 0;
        log.info("Transfer progress — file={} {}/{} bytes ({}%)",
                transferState.getRequest().getFileName(),
                transferState.getBytesTransferred(),
                transferState.getTotalBytes(),
                pct);
    }

    @Override
    public String detectTransferInterruption(TransferState transferState) {
        long offset     = transferState.getBytesTransferred();
        long total      = transferState.getTotalBytes();
        String fileName = transferState.getRequest().getFileName();
        String status   = offset == 0 ? "NOT_STARTED" : (offset >= total && total > 0 ? "COMPLETE" : "INTERRUPTED");
        log.warn("Transfer interruption detected — file={} status={} bytesTransferred={}/{}",
                fileName, status, offset, total);
        return status;
    }
}
