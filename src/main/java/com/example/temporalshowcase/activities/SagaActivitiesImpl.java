package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

@Slf4j
@Component
public class SagaActivitiesImpl implements SagaActivities {

    @Value("${app.data.dir:./data}")
    private String dataDir;

    private Path resolve(String filePath) {
        Path p = Paths.get(filePath);
        return p.isAbsolute() ? p : Paths.get(dataDir).resolve(filePath);
    }

    private Path quarantineDir() {
        return Paths.get(dataDir, "quarantine");
    }

    // ──────────────────────────────────────────────────────────────

    /** Copies the corrupted file into data/quarantine/ for safe keeping. */
    @Override
    public void isolateCorruptedFile(FileMetadata fileMetadata) {
        Path source = resolve(fileMetadata.getFilePath());
        Path dest = quarantineDir().resolve(source.getFileName());
        try {
            Files.createDirectories(quarantineDir());
            if (Files.exists(source)) {
                Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
                log.warn("QUARANTINE — copied {} → {}", source, dest);
            } else {
                log.warn("QUARANTINE — source file not found ({}), creating placeholder", source);
                Files.writeString(dest,
                        "Quarantine placeholder for: " + fileMetadata.getFileName(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            }
        } catch (IOException e) {
            log.error("Failed to isolate file {}: {}", source, e.getMessage());
        }
    }

    @Override
    public void alertOperationsTeam(FileMetadata fileMetadata, CorruptionDetails corruptionDetails) {
        log.warn("ALERT OPS — file={} corruptionType={} offset={} expectedChecksum={} actualChecksum={}",
                fileMetadata.getFilePath(),
                corruptionDetails.getCorruptionType(),
                corruptionDetails.getCorruptedOffset(),
                corruptionDetails.getExpectedChecksum(),
                corruptionDetails.getActualChecksum());
    }

    /** Attempts to repair the file by removing lines with CORRUPT markers,
     *  writing the cleaned content to a .repaired copy. */
    @Override
    public RepairResult attemptFileRepair(FileMetadata fileMetadata, CorruptionDetails corruptionDetails) {
        Path source = resolve(fileMetadata.getFilePath());
        Path repaired = source.resolveSibling(source.getFileName() + ".repaired");
        log.info("Attempting repair of {} (type={})", source, corruptionDetails.getCorruptionType());
        try {
            if (!Files.exists(source)) {
                return RepairResult.failure("Source file not found");
            }
            long removed = 0;
            var lines = Files.readAllLines(source, StandardCharsets.UTF_8);
            var clean = new java.util.ArrayList<String>();
            for (String line : lines) {
                if (line.contains("CORRUPT") || line.contains("???INVALID???") || line.contains("INVALID_DATE")) {
                    removed++;
                    log.debug("Removed corrupt line: {}", line);
                } else {
                    clean.add(line);
                }
            }
            Files.write(repaired, clean, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            log.info("Repair complete — removed {} corrupt lines, wrote {}", removed, repaired);

            FileMetadata repairedMeta = FileMetadata.builder()
                    .filePath(repaired.toAbsolutePath().normalize().toString())
                    .fileName(repaired.getFileName().toString())
                    .fileSize(Files.size(repaired))
                    .fileChecksum(fileMetadata.getFileChecksum())
                    .fileType(fileMetadata.getFileType())
                    .sourceSystem(fileMetadata.getSourceSystem())
                    .build();
            return RepairResult.success(repairedMeta, "CORRUPT_LINE_REMOVAL");
        } catch (IOException e) {
            log.error("Repair failed for {}: {}", source, e.getMessage());
            return RepairResult.failure("IO error during repair: " + e.getMessage());
        }
    }

    @Override
    public ProcessingResult resumeProcessingAfterRepair(FileMetadata repairedFile) {
        Path path = resolve(repairedFile.getFilePath());
        log.info("Resuming processing on repaired file: {}", path);
        if (!Files.exists(path)) {
            return ProcessingResult.builder()
                    .status(ProcessingStatus.FAILED)
                    .message("Repaired file not found: " + path)
                    .recordCount(0).build();
        }
        try {
            long lines = Files.lines(path, StandardCharsets.UTF_8)
                    .filter(l -> !l.isBlank())
                    .count();
            long records = Math.max(0, lines - 1); // subtract header
            return ProcessingResult.builder()
                    .status(ProcessingStatus.COMPLETED)
                    .message("Repaired file processed — " + records + " clean records from " + path.getFileName())
                    .recordCount(records)
                    .build();
        } catch (IOException e) {
            return ProcessingResult.builder()
                    .status(ProcessingStatus.FAILED)
                    .message("IO error reading repaired file: " + e.getMessage())
                    .recordCount(0).build();
        }
    }

    @Override
    public void requestFileResend(FileMetadata fileMetadata) {
        log.info("RESEND REQUEST — file={} source={}", fileMetadata.getFileName(), fileMetadata.getSourceSystem());
    }

    /** Restores the original file by removing any .repaired copy. */
    @Override
    public void restoreFileState(FileMetadata fileMetadata) {
        Path source = resolve(fileMetadata.getFilePath());
        Path repaired = source.resolveSibling(source.getFileName() + ".repaired");
        try {
            if (Files.exists(repaired)) {
                Files.delete(repaired);
                log.info("Restored state — deleted repaired copy: {}", repaired);
            }
        } catch (IOException e) {
            log.warn("Could not remove repaired copy {}: {}", repaired, e.getMessage());
        }
    }

    @Override
    public void rollbackDatabaseTransactions(FileMetadata fileMetadata) {
        log.info("ROLLBACK DB — rolling back transactions for file: {}", fileMetadata.getFileName());
    }

    @Override
    public void cancelDownstreamProcessing(FileMetadata fileMetadata) {
        log.info("CANCEL DOWNSTREAM — cancelling downstream jobs for file: {}", fileMetadata.getFileName());
    }

    @Override
    public void notifyConsumersOfFailure(FileMetadata fileMetadata, String reason) {
        log.warn("CONSUMER NOTIFICATION — file={} reason={}", fileMetadata.getFileName(), reason);
    }

    @Override
    public void logCompensationExecution(Compensation compensation, String status) {
        log.info("COMPENSATION — activity={} status={}",
                compensation.getActivityName(), status);
    }

    @Override
    public void updateProcessingStatus(FileMetadata fileMetadata, String status, String reason) {
        log.info("STATUS UPDATE — file={} status={} reason={}",
                fileMetadata.getFileName(), status, reason);
    }
}
