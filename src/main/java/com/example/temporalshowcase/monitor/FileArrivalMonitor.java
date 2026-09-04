package com.example.temporalshowcase.monitor;

import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.workflows.pattern1.FileProcessingWorkflow;
import io.temporal.client.WorkflowClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Showcase stand-in for an SFG / SFTP file-monitor service.
 * After {@code start-waiting}, watches {@code data/incoming} and sends
 * {@code fileArrived} when the expected file is created or updated
 * (last-modified at or after the watch was registered).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FileArrivalMonitor {

    private final WorkflowClient workflowClient;

    @Value("${app.data.dir:./data}")
    private String dataDir;

    @Value("${app.poller.enabled:true}")
    private boolean enabled;

    private final ConcurrentHashMap<String, Watch> waiting = new ConcurrentHashMap<>();

    public void watch(String workflowId, String fileName) {
        waiting.put(workflowId, new Watch(fileName, System.currentTimeMillis()));
        log.info("File monitor watching incoming/{} for workflow {}", fileName, workflowId);
    }

    public void forget(String workflowId) {
        waiting.remove(workflowId);
    }

    @Scheduled(fixedDelayString = "${app.poller.interval-ms:3000}")
    public void poll() {
        if (!enabled || waiting.isEmpty()) return;
        Path incoming = Path.of(dataDir).resolve("incoming");
        for (Map.Entry<String, Watch> e : waiting.entrySet()) {
            String workflowId = e.getKey();
            Watch watch = e.getValue();
            Path file = incoming.resolve(watch.fileName());
            try {
                if (!Files.exists(file) || Files.size(file) == 0) continue;
                long mtime = Files.getLastModifiedTime(file).toMillis();
                if (mtime + 1000 < watch.registeredAt()) continue;

                FileMetadata meta = FileMetadata.builder()
                        .filePath(file.toAbsolutePath().normalize().toString())
                        .fileName(watch.fileName())
                        .fileSize(Files.size(file))
                        .fileType("BATCH")
                        .arrivalTimestamp(System.currentTimeMillis())
                        .sourceSystem("FILE_MONITOR")
                        .build();
                FileProcessingWorkflow stub = workflowClient.newWorkflowStub(
                        FileProcessingWorkflow.class, workflowId);
                stub.fileArrived(meta);
                waiting.remove(workflowId);
                log.info("File monitor signaled fileArrived workflow={} file={}", workflowId, watch.fileName());
            } catch (Exception ex) {
                log.warn("File monitor could not signal {} for {}: {}", workflowId, watch.fileName(), ex.getMessage());
            }
        }
    }

    private record Watch(String fileName, long registeredAt) {}
}
