package com.example.temporalshowcase.sftp;

import com.example.temporalshowcase.config.TemporalProperties;
import com.example.temporalshowcase.config.TemporalRuntime;
import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.workflows.pattern1.FileProcessingWorkflow;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowOptions;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

/**
 * Pattern 1 external SFG poller. When {@code temporal.sftp.enabled=true}, it polls
 * the remote SFTP directory, performs an SFTP GET for each new file, then starts
 * the FileProcessing workflow and sends it the {@code fileArrived} signal — i.e.
 * this plays the role of the real SFG file monitor that lives outside Temporal.
 *
 * Disabled by default, so local/Docker runs without an SFTP server are unaffected.
 * The poller is a Temporal client (not a workflow/activity), which is the correct
 * place for the non-deterministic "watch the filesystem" responsibility.
 */
@Slf4j
@Component
@DependsOn("temporalRuntime")
@RequiredArgsConstructor
public class SftpFilePoller {

    private final WorkflowClient workflowClient;
    private final TemporalProperties props;
    private final Set<String> seen = new HashSet<>();

    @PostConstruct
    public void start() {
        TemporalProperties.Sftp cfg = props.getSftp();
        if (!cfg.isEnabled()) {
            log.info("SFTP poller disabled (temporal.sftp.enabled=false)");
            return;
        }
        Thread t = new Thread(() -> pollLoop(cfg), "sftp-file-poller");
        t.setDaemon(true);
        t.start();
        log.info("SFTP poller started: {}:{} dir={} interval={}",
                cfg.getHost(), cfg.getPort(), cfg.getRemoteDir(), cfg.getPollInterval());
    }

    private void pollLoop(TemporalProperties.Sftp cfg) {
        SftpGateway gateway = new SftpGateway(cfg);
        while (!Thread.currentThread().isInterrupted()) {
            try {
                for (String name : gateway.listRemoteFiles()) {
                    if (seen.contains(name)) {
                        continue;
                    }
                    Path local = gateway.download(name);   // SFTP GET
                    startWorkflow(name, local, cfg);
                    seen.add(name);
                }
            } catch (Exception e) {
                log.warn("SFTP poll cycle failed: {}", e.getMessage());
            }
            try {
                Thread.sleep(cfg.getPollInterval().toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void startWorkflow(String name, Path local, TemporalProperties.Sftp cfg) {
        FileMetadata file = FileMetadata.builder()
                .filePath(local.toAbsolutePath().toString())
                .fileName(name)
                .sourceSystem("SFG_SFTP")
                .slaTimeoutSeconds(cfg.getSla().getSeconds())
                .build();
        String wfId = "p01-sftp-" + name;
        FileProcessingWorkflow wf = workflowClient.newWorkflowStub(
                FileProcessingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(wfId)
                        .setTaskQueue(TemporalRuntime.queue("delayed-file"))
                        .build());
        try {
            WorkflowClient.start(wf::processFile, file);
            wf.fileArrived(file); // signal that the downloaded file has arrived
            log.info("SFTP poller started workflow {} for {}", wfId, name);
        } catch (WorkflowExecutionAlreadyStarted e) {
            log.info("Workflow {} already started for {} — skipping", wfId, name);
        }
    }
}
