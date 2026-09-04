package com.example.temporalshowcase.controller;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.models.*;
import com.example.temporalshowcase.monitor.FileArrivalMonitor;
import com.example.temporalshowcase.workflows.pattern1.FileProcessingWorkflow;
import com.example.temporalshowcase.workflows.pattern10.MultiStageValidationWorkflow;
import com.example.temporalshowcase.workflows.pattern11.DistributedLockingWorkflow;
import com.example.temporalshowcase.workflows.pattern11.LockWaitWorkflow;
import com.example.temporalshowcase.workflows.pattern12.DependencyGraphWorkflow;
import com.example.temporalshowcase.workflows.pattern2.LargeFileProcessingWorkflow;
import com.example.temporalshowcase.workflows.pattern3.CircuitBreakerWorkflow;
import com.example.temporalshowcase.workflows.pattern4.CorruptedFileSagaWorkflow;
import com.example.temporalshowcase.workflows.pattern5.IdempotentFileProcessingWorkflow;
import com.example.temporalshowcase.workflows.pattern6.MultiFormatHandlerWorkflow;
import com.example.temporalshowcase.workflows.pattern7.PermissionEscalationWorkflow;
import com.example.temporalshowcase.workflows.pattern8.NormalTransferWorkflow;
import com.example.temporalshowcase.workflows.pattern8.ResumableTransferWorkflow;
import com.example.temporalshowcase.workflows.pattern9.NormalProcessingWorkflow;
import com.example.temporalshowcase.workflows.pattern9.RateLimitingWorkflow;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST controller providing one endpoint per Temporal showcase pattern.
 * Each endpoint starts the corresponding workflow and returns its run ID
 * so callers can query or signal it later via the Temporal Web UI or SDK.
 */
@Slf4j
@RestController
@RequestMapping("/api/showcase")
@RequiredArgsConstructor
public class ShowcaseController {

    private final WorkflowClient workflowClient;
    private final FileArrivalMonitor fileArrivalMonitor;

    @Value("${app.data.dir:./data}")
    private String dataDir;

    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 1 – Signal-based File Arrival Detection
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Starts the workflow and immediately sends the fileArrived signal so the
     * showcase works end-to-end without an external signal sender.
     */
    @PostMapping("/pattern1/signal-arrival")
    public ResponseEntity<Map<String, String>> startSignalArrival(
            @RequestBody(required = false) FileMetadata fileMetadata) {

        FileMetadata file = defaultFile(fileMetadata, "payment_batch_2026.csv");
        String workflowId = "p01-signal-" + UUID.randomUUID();

        FileProcessingWorkflow stub = workflowClient.newWorkflowStub(
                FileProcessingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("delayed-file"))
                        .build());

        // Start async, then send the fileArrived signal immediately
        WorkflowClient.start(stub::processFile, file);
        stub.fileArrived(file);

        log.info("Pattern 1 started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 1: Signal-based Arrival started");
    }

    /**
     * Pattern 1 Step A — starts the workflow and leaves it WAITING for the signal.
     * This simulates the real flow where the workflow is pre-started before the
     * file monitor detects the file.
     *
     * Example:
     *   POST /api/showcase/pattern1/start-waiting
     *   → returns workflowId, then use that in /pattern1/send-signal/{workflowId}
     */
    @PostMapping("/pattern1/start-waiting")
    public ResponseEntity<Map<String, String>> startWaiting(
            @RequestParam(defaultValue = "payment_batch_2026.csv") String fileName) {

        FileMetadata file = defaultFile(null, fileName);
        String workflowId = "p01-wait-" + UUID.randomUUID();

        FileProcessingWorkflow stub = workflowClient.newWorkflowStub(
                FileProcessingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("delayed-file"))
                        .build());

        WorkflowClient.start(stub::processFile, file);
        fileArrivalMonitor.watch(workflowId, fileName);
        log.info("Pattern 1 waiting for signal: workflowId={}", workflowId);
        return ok(workflowId,
                "Pattern 1: WAITING_FOR_FILE_SIGNAL — file monitor will signal when incoming/"
                        + fileName + " is created or touched, or POST /pattern1/send-signal/" + workflowId);
    }

    /**
     * Pattern 1 Step B — simulates the file monitor detecting the file
     * and sending the fileArrived signal to the waiting workflow.
     *
     * Example:
     *   POST /api/showcase/pattern1/send-signal/{workflowId}?fileName=payment_batch_2026.csv
     */
    @PostMapping("/pattern1/send-signal/{workflowId}")
    public ResponseEntity<Map<String, String>> sendFileArrivedSignal(
            @PathVariable String workflowId,
            @RequestParam(defaultValue = "payment_batch_2026.csv") String fileName) {

        FileMetadata file = defaultFile(null, fileName);
        FileProcessingWorkflow stub = workflowClient.newWorkflowStub(
                FileProcessingWorkflow.class, workflowId);
        stub.fileArrived(file);

        log.info("Pattern 1 fileArrived signal sent: workflowId={} file={}", workflowId, fileName);
        return ok(workflowId, "fileArrived signal sent for file: " + fileName);
    }

    /**
     * Pattern 1 — SLA Timeout test endpoint.
     *
     * Starts the workflow WITHOUT sending the fileArrived signal so the SLA
     * timer fires after {@code slaSeconds} seconds (default 30 s).
     * Watch the workflow in the Temporal UI — it will transition to FAILED
     * with error type SLA_TIMEOUT once the timer expires.
     *
     * Example:
     *   POST /api/showcase/pattern1/timeout-test?slaSeconds=30
     */
    @PostMapping("/pattern1/timeout-test")
    public ResponseEntity<Map<String, String>> startSignalTimeout(
            @RequestParam(defaultValue = "30") long slaSeconds) {

        FileMetadata file = buildFile("incoming/missing_file.csv", "missing_file.csv", 0);
        file.setSlaTimeoutSeconds(slaSeconds);   // override the default 24 h
        String workflowId = "p01-timeout-" + UUID.randomUUID();

        FileProcessingWorkflow stub = workflowClient.newWorkflowStub(
                FileProcessingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("delayed-file"))
                        .build());

        // Start WITHOUT sending fileArrived — the SLA timer will fire
        WorkflowClient.start(stub::processFile, file);

        log.info("Pattern 1 SLA timeout test started: workflowId={} slaSeconds={}", workflowId, slaSeconds);
        return ok(workflowId,
                "Pattern 1: SLA timeout test started — workflow will fail with SLA_TIMEOUT in " + slaSeconds + "s");
    }

    /**
     * Pattern 1 — Query the live processing status of a running workflow.
     *
     * Example:
     *   GET /api/showcase/pattern1/status/{workflowId}
     */
    @GetMapping("/pattern1/status/{workflowId}")
    public ResponseEntity<Map<String, String>> getPattern1Status(@PathVariable String workflowId) {
        FileProcessingWorkflow stub = workflowClient.newWorkflowStub(
                FileProcessingWorkflow.class, workflowId);
        String status = stub.getProcessingStatus();
        return ResponseEntity.ok(Map.of(
                "workflowId", workflowId,
                "processingStatus", status));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 2 – Large File Chunked Processing
    // ─────────────────────────────────────────────────────────────────────────

    @PostMapping("/pattern2/large-file")
    public ResponseEntity<Map<String, String>> startLargeFileProcessing(
            @RequestBody(required = false) FileMetadata fileMetadata) {

        FileMetadata file = defaultFile(fileMetadata, "large_transaction_dump.dat");
        // Use real file size so chunking reflects the actual bytes on disk
        String workflowId = "p02-large-" + UUID.randomUUID();

        LargeFileProcessingWorkflow stub = workflowClient.newWorkflowStub(
                LargeFileProcessingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("large-file"))
                        .build());

        WorkflowClient.start(stub::processLargeFile, file);
        log.info("Pattern 2 started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 2: Large File Chunked Processing started");
    }

    /** Query live chunk progress for a running Pattern 2 workflow. */
    @GetMapping("/pattern2/progress/{workflowId}")
    public ResponseEntity<Map<String, String>> getPattern2Progress(@PathVariable String workflowId) {
        LargeFileProcessingWorkflow stub = workflowClient.newWorkflowStub(
                LargeFileProcessingWorkflow.class, workflowId);
        String progress = stub.getProgress();
        return ResponseEntity.ok(Map.of("workflowId", workflowId, "progress", progress));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 3 – Circuit Breaker (Empty File)
    // ─────────────────────────────────────────────────────────────────────────

    @PostMapping("/pattern3/circuit-breaker")
    public ResponseEntity<Map<String, String>> startCircuitBreaker(
            @RequestBody(required = false) FileMetadata fileMetadata) {

        FileMetadata file = defaultFile(fileMetadata, "possibly_empty.csv");
        file.setFileSize(0); // force empty-file scenario
        String workflowId = "p03-cb-" + UUID.randomUUID();

        CircuitBreakerWorkflow stub = workflowClient.newWorkflowStub(
                CircuitBreakerWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("circuit-breaker"))
                        .build());

        WorkflowClient.start(stub::processFileWithCircuitBreaker, file);
        log.info("Pattern 3 started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 3: Circuit Breaker started");
    }

    @GetMapping("/pattern3/circuit-state/{workflowId}")
    public ResponseEntity<Map<String, String>> getCircuitState(@PathVariable String workflowId) {
        CircuitBreakerWorkflow stub = workflowClient.newWorkflowStub(
                CircuitBreakerWorkflow.class, workflowId);
        String state = stub.getCircuitState();
        return ResponseEntity.ok(Map.of("workflowId", workflowId, "circuitState", state));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 4 – Corrupted File Saga Recovery
    // ─────────────────────────────────────────────────────────────────────────

    @PostMapping("/pattern4/saga-recovery")
    public ResponseEntity<Map<String, String>> startSagaRecovery(
            @RequestBody(required = false) FileMetadata fileMetadata) {

        FileMetadata file = defaultFile(fileMetadata, "corrupted_claims.xml");
        CorruptionDetails corruption = CorruptionDetails.builder()
                .corruptionType("CHECKSUM_MISMATCH")
                .expectedChecksum("abc123")
                .actualChecksum("xyz999")
                .description("File checksum does not match expected value")
                .build();
        String workflowId = "p04-saga-" + UUID.randomUUID();

        CorruptedFileSagaWorkflow stub = workflowClient.newWorkflowStub(
                CorruptedFileSagaWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("saga-recovery"))
                        .build());

        WorkflowClient.start(stub::handleCorruptedFile, file, corruption);
        log.info("Pattern 4 started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 4: Saga Recovery started");
    }

    @GetMapping("/pattern4/saga-status/{workflowId}")
    public ResponseEntity<Map<String, String>> getSagaStatus(@PathVariable String workflowId) {
        CorruptedFileSagaWorkflow stub = workflowClient.newWorkflowStub(
                CorruptedFileSagaWorkflow.class, workflowId);
        String status = stub.getSagaStatus();
        return ResponseEntity.ok(Map.of("workflowId", workflowId, "sagaStatus", status));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 5 – Idempotent File Processing
    // ─────────────────────────────────────────────────────────────────────────

    @PostMapping("/pattern5/idempotency")
    public ResponseEntity<Map<String, String>> startIdempotency(
            @RequestBody(required = false) FileMetadata fileMetadata) {

        FileMetadata file = defaultFile(fileMetadata, "daily_trades_20260831.csv");
        String workflowId = "p05-idem-" + UUID.randomUUID();

        IdempotentFileProcessingWorkflow stub = workflowClient.newWorkflowStub(
                IdempotentFileProcessingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("idempotency"))
                        .build());

        WorkflowClient.start(stub::processFileWithIdempotency, file);
        log.info("Pattern 5 started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 5: Idempotent Processing started");
    }

    @GetMapping("/pattern5/dedup-state/{workflowId}")
    public ResponseEntity<Map<String, String>> getDedupState(
            @PathVariable String workflowId,
            @RequestParam String fileHash) {
        IdempotentFileProcessingWorkflow stub = workflowClient.newWorkflowStub(
                IdempotentFileProcessingWorkflow.class, workflowId);
        String state = stub.getDeduplicationState(fileHash);
        return ResponseEntity.ok(Map.of("workflowId", workflowId, "fileHash", fileHash, "dedupState", state));
    }

    @PostMapping("/pattern5/duplicate")
    public ResponseEntity<Map<String, String>> startDuplicateIdempotency(
            @RequestBody(required = false) FileMetadata fileMetadata) {
        // Sends the same file a second time — the workflow will detect the hash match
        // and return the cached result (Step 3: Skip Processing, Return previous result)
        FileMetadata file = defaultFile(fileMetadata, "daily_trades_20260831.csv");
        String workflowId = "p05-idem-dup-" + UUID.randomUUID();
        IdempotentFileProcessingWorkflow stub = workflowClient.newWorkflowStub(
                IdempotentFileProcessingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("idempotency"))
                        .build());
        WorkflowClient.start(stub::processFileWithIdempotency, file);
        log.info("Pattern 5 duplicate send started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 5: Duplicate send started — should return cached result");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 6 – Multi-Format Handler
    // ─────────────────────────────────────────────────────────────────────────

    @PostMapping("/pattern6/multi-format")
    public ResponseEntity<Map<String, String>> startMultiFormat(
            @RequestParam(defaultValue = "CSV") String format,
            @RequestBody(required = false) FileMetadata fileMetadata) {

        String ext = format.equalsIgnoreCase("XML") ? ".xml"
                : format.equalsIgnoreCase("JSON") ? ".json"
                : format.equalsIgnoreCase("FIXED_WIDTH") ? ".dat"
                : ".csv";
        // Map to the correct sample file in data/incoming/
        String sampleFile = format.equalsIgnoreCase("XML")        ? "records.xml"
                : format.equalsIgnoreCase("JSON")      ? "records.json"
                : format.equalsIgnoreCase("FIXED_WIDTH") ? "large_transaction_dump.dat"
                : "records.csv";

        FileMetadata file = defaultFile(fileMetadata, sampleFile);
        String workflowId = "p06-fmt-" + UUID.randomUUID();

        MultiFormatHandlerWorkflow stub = workflowClient.newWorkflowStub(
                MultiFormatHandlerWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("multi-format"))
                        .build());

        WorkflowClient.start(stub::handleMultiFormatFile, file);
        log.info("Pattern 6 started: workflowId={} format={}", workflowId, format);
        return ok(workflowId, "Pattern 6: Multi-Format Handler started with format=" + format);
    }

    @GetMapping("/pattern6/format/{workflowId}")
    public ResponseEntity<Map<String, String>> getCurrentFormat(@PathVariable String workflowId) {
        MultiFormatHandlerWorkflow stub = workflowClient.newWorkflowStub(
                MultiFormatHandlerWorkflow.class, workflowId);
        String fmt = stub.getCurrentFormat();
        return ResponseEntity.ok(Map.of("workflowId", workflowId, "currentFormat", fmt));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 7 – Permission Escalation
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Scenario 1 — Permission OK → Normal Processing (file starts with "allowed_" or known clean file).
     */
    @PostMapping("/pattern7/scenario1-permission-ok")
    public ResponseEntity<Map<String, String>> startPermissionOk() {
        FileMetadata file = buildFile("incoming/payment_batch_2026.csv", "payment_batch_2026.csv", 1024);
        return startP7Workflow(file, "ACCESS_OK", "File has correct permissions");
    }

    /**
     * Scenario 2 — Permission Denied + Auto Fix Success.
     * errorCode != STRICT so applyStandardPermissions succeeds.
     */
    @PostMapping("/pattern7/scenario2-autofix")
    public ResponseEntity<Map<String, String>> startPermissionAutoFix() {
        FileMetadata file = buildFile("incoming/restricted_payroll.csv", "restricted_payroll.csv", 512);
        return startP7Workflow(file, "ACCESS_DENIED", "Service account lacks read permission");
    }

    /**
     * Scenario 3 — Permission Denied + All Auto Fixes Fail → Human Intervention required.
     * errorCode = STRICT forces all 4 strategies to fail, workflow waits for signal.
     */
    @PostMapping("/pattern7/scenario3-human-intervention")
    public ResponseEntity<Map<String, String>> startPermissionHumanIntervention() {
        FileMetadata file = buildFile("incoming/restricted_payroll.csv", "restricted_payroll.csv", 512);
        return startP7Workflow(file, "STRICT", "Highly restricted file — all auto-fixes blocked");
    }

    /**
     * Scenario 4 — Send permissionResolved signal (after Scenario 3 is running).
     * Use workflowId from Scenario 3 response.
     */
    @PostMapping("/pattern7/scenario4-resolve/{workflowId}")
    public ResponseEntity<Map<String, String>> resolvePermission(
            @PathVariable String workflowId,
            @RequestParam(defaultValue = "MANUALLY_FIXED_BY_ADMIN") String resolution) {
        PermissionEscalationWorkflow stub = workflowClient.newWorkflowStub(
                PermissionEscalationWorkflow.class, workflowId);
        stub.permissionResolved(resolution);
        log.info("Pattern 7 permissionResolved signal sent to workflowId={} resolution={}", workflowId, resolution);
        return ok(workflowId, "permissionResolved signal sent: " + resolution);
    }

    /**
     * Scenario 5 — Query live escalation status at any point.
     */
    @GetMapping("/pattern7/escalation-status/{workflowId}")
    public ResponseEntity<Map<String, String>> getEscalationStatus(@PathVariable String workflowId) {
        PermissionEscalationWorkflow stub = workflowClient.newWorkflowStub(
                PermissionEscalationWorkflow.class, workflowId);
        String status = stub.getEscalationStatus();
        return ResponseEntity.ok(Map.of("workflowId", workflowId, "escalationStatus", status));
    }

    private ResponseEntity<Map<String, String>> startP7Workflow(
            FileMetadata file, String errorCode, String description) {
        PermissionIssue issue = PermissionIssue.builder()
                .filePath(file.getFilePath())
                .requiredPermission("READ")
                .currentPermission("NONE")
                .errorCode(errorCode)
                .description(description)
                .build();
        String workflowId = "p07-perm-" + UUID.randomUUID();
        PermissionEscalationWorkflow stub = workflowClient.newWorkflowStub(
                PermissionEscalationWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("permission-escalation"))
                        .build());
        WorkflowClient.start(stub::resolvePermissionIssue, file, issue);
        log.info("Pattern 7 started: workflowId={} errorCode={}", workflowId, errorCode);
        return ok(workflowId, "Pattern 7 started — errorCode=" + errorCode);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 8 – Resumable Transfer
    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 8 – Resumable Transfer
    // ─────────────────────────────────────────────────────────────────────────

    /** Scenario 1 – Network OK: direct single-shot transfer, no checkpointing. */
    @PostMapping("/pattern8/normal-transfer")
    public ResponseEntity<Map<String, String>> startNormalTransfer() {
        var srcPath = Paths.get(dataDir).resolve("incoming/large_transaction_dump.dat").toAbsolutePath().normalize();
        long srcSize = 0;
        try { if (Files.exists(srcPath)) srcSize = Files.size(srcPath); } catch (IOException ignored) {}
        TransferRequest request = TransferRequest.builder()
                .sourceUrl("file://" + srcPath)
                .destinationPath(srcPath.toString())
                .fileName("large_transaction_dump.dat")
                .expectedFileSize(srcSize)
                .expectedChecksum("sha256-expected")
                .maxRetries(3)
                .build();
        String workflowId = "p08-normal-" + UUID.randomUUID();
        NormalTransferWorkflow stub = workflowClient.newWorkflowStub(
                NormalTransferWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("resumable-transfer"))
                        .build());
        WorkflowClient.start(stub::executeNormalTransfer, request);
        log.info("Pattern 8 normal transfer started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 8: Normal Transfer started (Network OK path)");
    }

    /**
     * Scenario 2 – Network Failure then Recovery:
     * First 2 connectivity checks return DISCONNECTED → detect interruption → save checkpoint
     * → wait → resume from checkpoint → validate integrity → complete.
     */
    @PostMapping("/pattern8/resumable-transfer")
    public ResponseEntity<Map<String, String>> startResumableTransfer() {
        var srcPath = Paths.get(dataDir).resolve("incoming/large_transaction_dump.dat").toAbsolutePath().normalize();
        long srcSize = 0;
        try { if (Files.exists(srcPath)) srcSize = Files.size(srcPath); } catch (IOException ignored) {}
        TransferRequest request = TransferRequest.builder()
                .sourceUrl("file://" + srcPath)
                .destinationPath(srcPath.toString())
                .fileName("large_transaction_dump.dat")
                .expectedFileSize(srcSize)
                .expectedChecksum("sha256-expected")
                .maxRetries(5)
                .simulateNetworkFailureCount(2) // first 2 checks return DISCONNECTED
                .build();
        String workflowId = "p08-transfer-" + UUID.randomUUID();
        ResumableTransferWorkflow stub = workflowClient.newWorkflowStub(
                ResumableTransferWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("resumable-transfer"))
                        .build());
        WorkflowClient.start(stub::executeResumableTransfer, request);
        log.info("Pattern 8 resumable transfer started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 8: Resumable Transfer started (Network Failure → Recovery path)");
    }

    /**
     * Scenario 3 – Integrity Failure → Restart from Beginning (Step 7):
     * Transfer completes but first integrity check fails → workflow restarts from byte 0.
     */
    @PostMapping("/pattern8/integrity-failure")
    public ResponseEntity<Map<String, String>> startIntegrityFailureTransfer() {
        var srcPath = Paths.get(dataDir).resolve("incoming/large_transaction_dump.dat").toAbsolutePath().normalize();
        long srcSize = 0;
        try { if (Files.exists(srcPath)) srcSize = Files.size(srcPath); } catch (IOException ignored) {}
        TransferRequest request = TransferRequest.builder()
                .sourceUrl("file://" + srcPath)
                .destinationPath(srcPath.toString())
                .fileName("large_transaction_dump_integrity.dat")
                .expectedFileSize(srcSize)
                .expectedChecksum("sha256-expected")
                .maxRetries(5)
                .simulateIntegrityFailure(true) // first integrity check returns false → Step 7
                .build();
        String workflowId = "p08-integrity-" + UUID.randomUUID();
        ResumableTransferWorkflow stub = workflowClient.newWorkflowStub(
                ResumableTransferWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("resumable-transfer"))
                        .build());
        WorkflowClient.start(stub::executeResumableTransfer, request);
        log.info("Pattern 8 integrity-failure transfer started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 8: Integrity Failure Transfer started (Step 7 restart path)");
    }

    /** Query live transfer progress for a running resumable transfer. */
    @GetMapping("/pattern8/progress/{workflowId}")
    public ResponseEntity<Map<String, String>> getTransferProgress(@PathVariable String workflowId) {
        ResumableTransferWorkflow stub = workflowClient.newWorkflowStub(
                ResumableTransferWorkflow.class, workflowId);
        String progress = stub.getTransferProgress();
        return ResponseEntity.ok(Map.of("workflowId", workflowId, "progress", progress));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 9 – Rate Limiting
    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 9 – Rate Limiting
    // ─────────────────────────────────────────────────────────────────────────

    /** Scenario 1 – Within Limits: load is low, normal processing, no throttling. */
    @PostMapping("/pattern9/within-limits")
    public ResponseEntity<Map<String, String>> startNormalProcessing(
            @RequestParam(defaultValue = "10") int filesToProcess) {
        String workflowId = "p09-normal-" + UUID.randomUUID();
        NormalProcessingWorkflow stub = workflowClient.newWorkflowStub(
                NormalProcessingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("rate-limiting"))
                        .build());
        WorkflowClient.start(stub::executeNormalProcessing, filesToProcess);
        log.info("Pattern 9 normal processing started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 9: Normal Processing started (Within Limits path)");
    }

    /**
     * Scenario 2 – Rate Limit Exceeded: adaptive throttling kicks in.
     * Runs 3 monitoring cycles with Steps 1–7 (queue files → wait → release/escalate).
     */
    @PostMapping("/pattern9/rate-exceeded")
    public ResponseEntity<Map<String, String>> startRateLimiting(
            @RequestParam(defaultValue = "3") int cycles) {
        String workflowId = "p09-rate-" + UUID.randomUUID();
        RateLimitingWorkflow stub = workflowClient.newWorkflowStub(
                RateLimitingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("rate-limiting"))
                        .build());
        WorkflowClient.start(stub::manageRateLimiting, cycles);
        log.info("Pattern 9 rate limiting started: workflowId={} cycles={}", workflowId, cycles);
        return ok(workflowId, "Pattern 9: Adaptive Throttling started (" + cycles + " monitoring cycles)");
    }

    /** Query live throttle status for a running rate-limiting workflow. */
    @GetMapping("/pattern9/throttle-status/{workflowId}")
    public ResponseEntity<Map<String, String>> getThrottleStatus(@PathVariable String workflowId) {
        RateLimitingWorkflow stub = workflowClient.newWorkflowStub(
                RateLimitingWorkflow.class, workflowId);
        String status = stub.getCurrentThrottleStatus();
        return ResponseEntity.ok(Map.of("workflowId", workflowId, "throttleStatus", status));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 10 – Multi-Stage Validation
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Scenario 1 – Quality OK: clean file passes all 4 stages in one pass.
     * File: payment_batch_2026.csv (7 columns, 20 rows, no corruption)
     * Expected: BASIC→BUSINESS→ADVANCED→FINAL all PASS. Workflow completes cleanly.
     */
    @PostMapping("/pattern10/quality-ok")
    public ResponseEntity<Map<String, String>> startQualityOkProcessing() {
        FileMetadata file = buildFile("incoming/payment_batch_2026.csv", "payment_batch_2026.csv", 1024);
        String workflowId = "p10-ok-" + UUID.randomUUID();
        MultiStageValidationWorkflow stub = workflowClient.newWorkflowStub(
                MultiStageValidationWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("validation"))
                        .build());
        WorkflowClient.start(stub::executeMultiStageValidation, file);
        log.info("Pattern 10 quality-ok started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 10 Scenario 1: Quality OK — clean file, all 4 stages pass");
    }

    /**
     * Scenario 2 – Business Remediation: file has only 1 CSV column → Business validation fails.
     * File: malformed_batch.csv (1 column header, 3 rows — violates "must have ≥2 columns" rule)
     * Expected: BASIC PASS → BUSINESS FAIL → correctMissingData → approveProcessingAndMarkAsQuality → COMPLETED
     */
    @PostMapping("/pattern10/business-remediation")
    public ResponseEntity<Map<String, String>> startBusinessRemediation() {
        FileMetadata file = buildFile("incoming/malformed_batch.csv", "malformed_batch.csv", 256);
        String workflowId = "p10-business-" + UUID.randomUUID();
        MultiStageValidationWorkflow stub = workflowClient.newWorkflowStub(
                MultiStageValidationWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("validation"))
                        .build());
        WorkflowClient.start(stub::executeMultiStageValidation, file);
        log.info("Pattern 10 business-remediation started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 10 Scenario 2: Business Remediation — 1-column CSV fails business rules, triggers correctMissingData → approve");
    }

    /**
     * Scenario 3 – Advanced Validation fails 3 times → Manual Review / Escalate to data team.
     * File: corrupted_transactions.csv (contains CORRUPT + INVALID_DATE + XXX markers)
     * Expected: BASIC PASS → BUSINESS PASS → ADVANCED FAIL → detectAndCorrectOutliers (×3) → ESCALATION
     */
    @PostMapping("/pattern10/needs-remediation")
    public ResponseEntity<Map<String, String>> startValidationWithRemediation() {
        FileMetadata file = buildFile("incoming/corrupted_transactions.csv", "corrupted_transactions.csv", 512);
        String workflowId = "p10-remediate-" + UUID.randomUUID();
        MultiStageValidationWorkflow stub = workflowClient.newWorkflowStub(
                MultiStageValidationWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("validation"))
                        .build());
        WorkflowClient.start(stub::executeMultiStageValidation, file);
        log.info("Pattern 10 remediation started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 10 Scenario 3: Advanced Remediation — CORRUPT markers, 3 repair attempts → ESCALATION");
    }

    /** Query current validation stage for a running workflow. */
    @GetMapping("/pattern10/stage/{workflowId}")
    public ResponseEntity<Map<String, String>> getValidationStage(@PathVariable String workflowId) {
        MultiStageValidationWorkflow stub = workflowClient.newWorkflowStub(
                MultiStageValidationWorkflow.class, workflowId);
        String stage = stub.getCurrentValidationStage();
        return ResponseEntity.ok(Map.of("workflowId", workflowId, "currentStage", stage));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 11 – Distributed Locking
    // Scenarios:
    //   1. lock-acquired      → no contention, lock granted immediately → process → release → notify
    //   2. lock-wait-acquired → contention, LockWaitWorkflow polls + backoff → eventually acquires
    //   3. lock-wait-timeout  → contention, lock never released → LockWaitWorkflow times out → escalate
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Scenario 1 — Lock Acquired immediately (no contention).
     * File has high priority. Lock is free, so it is granted on the first attempt.
     * Flow: Acquire Lock → Process File → Release Lock → Notify Waiting Processes.
     */
    @PostMapping("/pattern11/lock-acquired")
    public ResponseEntity<Map<String, String>> p11LockAcquired(
            @RequestBody(required = false) FileMetadata fileMetadata) {

        FileMetadata file = (fileMetadata != null) ? fileMetadata :
                buildFile("incoming/daily_trades_20260831.csv", "daily_trades_20260831.csv", 0)
                        .toBuilder()
                        .fileChecksum("checksum-trades-001")
                        .sourceSystem("SFG_FTP")
                        .priority(5)
                        .build();

        String workflowId = "p11-acquired-" + UUID.randomUUID();
        DistributedLockingWorkflow stub = workflowClient.newWorkflowStub(
                DistributedLockingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("locking"))
                        .build());

        WorkflowClient.start(stub::processFileWithLock, file);
        log.info("Pattern 11 Scenario 1 (lock-acquired) started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 11 Scenario 1: Lock acquired immediately — no contention");
    }

    /**
     * Scenario 2 — Lock Denied then acquired via LockWaitWorkflow.
     * Starts TWO concurrent workflows on the SAME file (same lockKey).
     * The second workflow gets denied and goes through the LockWaitWorkflow path,
     * polls with exponential backoff, and eventually acquires after the first releases.
     */
    @PostMapping("/pattern11/lock-wait-acquired")
    public ResponseEntity<Map<String, String>> p11LockWaitAcquired() {
        String sharedChecksum = "checksum-shared-002";
        FileMetadata file = buildFile("incoming/payment_batch_2026.csv", "payment_batch_2026.csv", 0)
                .toBuilder()
                .fileChecksum(sharedChecksum)
                .sourceSystem("SFG_SFTP")
                .priority(3)
                .holdLockSeconds(8)
                .build();

        // Workflow 1 — will grab the lock first
        String wf1Id = "p11-wait-holder-" + UUID.randomUUID();
        DistributedLockingWorkflow stub1 = workflowClient.newWorkflowStub(
                DistributedLockingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(wf1Id)
                        .setTaskQueue(TemporalRuntime.queue("locking"))
                        .build());
        WorkflowClient.start(stub1::processFileWithLock, file);

        // Workflow 2 — starts slightly after; will hit Lock Denied → LockWaitWorkflow
        FileMetadata file2 = FileMetadata.builder()
                .filePath(file.getFilePath())
                .fileName(file.getFileName())
                .fileSize(file.getFileSize())
                .fileChecksum(sharedChecksum)
                .fileType("CSV")
                .arrivalTimestamp(System.currentTimeMillis())
                .sourceSystem("SFG_SFTP")
                .priority(2)
                .build();
        String wf2Id = "p11-wait-waiter-" + UUID.randomUUID();
        DistributedLockingWorkflow stub2 = workflowClient.newWorkflowStub(
                DistributedLockingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(wf2Id)
                        .setTaskQueue(TemporalRuntime.queue("locking"))
                        .build());
        WorkflowClient.start(stub2::processFileWithLock, file2);

        log.info("Pattern 11 Scenario 2 (lock-wait-acquired) started: holder={} waiter={}", wf1Id, wf2Id);
        return ResponseEntity.ok(Map.of(
                "holderWorkflowId", wf1Id,
                "waiterWorkflowId", wf2Id,
                "message", "Pattern 11 Scenario 2: waiter goes through LockWaitWorkflow (poll + backoff + acquire)",
                "queryLockStatus", "GET /api/showcase/pattern11/lock-status/" + wf2Id,
                "queryWaitStatus", "GET /api/showcase/pattern11/lock-wait-status/lock-wait-" + wf2Id
        ));
    }

    /**
     * Scenario 3 — Lock Denied, lock never released → LockWaitWorkflow times out and escalates.
     * Starts a holder workflow and then a waiter that will exhaust all backoff attempts.
     * Uses a unique checksum so the lock stays permanently held (holder workflow keeps the lock).
     */
    @PostMapping("/pattern11/lock-wait-timeout")
    public ResponseEntity<Map<String, String>> p11LockWaitTimeout() {
        String lockedChecksum = "checksum-locked-forever-" + UUID.randomUUID();
        FileMetadata holderFile = buildFile("incoming/daily_trades_20260831.csv", "daily_trades_20260831.csv", 0)
                .toBuilder()
                .fileChecksum(lockedChecksum)
                .sourceSystem("SFG_FTP")
                .priority(10)
                .holdLockSeconds(60)
                .build();

        // This workflow holds the lock
        String holderId = "p11-timeout-holder-" + UUID.randomUUID();
        DistributedLockingWorkflow holder = workflowClient.newWorkflowStub(
                DistributedLockingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(holderId)
                        .setTaskQueue(TemporalRuntime.queue("locking"))
                        .build());
        WorkflowClient.start(holder::processFileWithLock, holderFile);

        // Waiter workflow — will exhaust LockWaitWorkflow retries → timeout/escalate
        FileMetadata waiterFile = FileMetadata.builder()
                .filePath(holderFile.getFilePath())
                .fileName(holderFile.getFileName())
                .fileSize(holderFile.getFileSize())
                .fileChecksum(lockedChecksum)
                .fileType("CSV")
                .arrivalTimestamp(System.currentTimeMillis())
                .sourceSystem("SFG_FTP")
                .priority(1)
                .build();

        String waiterId = "p11-timeout-waiter-" + UUID.randomUUID();
        DistributedLockingWorkflow waiter = workflowClient.newWorkflowStub(
                DistributedLockingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(waiterId)
                        .setTaskQueue(TemporalRuntime.queue("locking"))
                        .build());
        WorkflowClient.start(waiter::processFileWithLock, waiterFile);

        log.info("Pattern 11 Scenario 3 (lock-wait-timeout) started: holder={} waiter={}", holderId, waiterId);
        return ResponseEntity.ok(Map.of(
                "holderWorkflowId", holderId,
                "waiterWorkflowId", waiterId,
                "message", "Pattern 11 Scenario 3: waiter will exhaust all retries → LOCK_WAIT_TIMEOUT escalation",
                "queryWaiterStatus", "GET /api/showcase/pattern11/lock-status/" + waiterId
        ));
    }

    /** Query current lock status for a DistributedLockingWorkflow. */
    @GetMapping("/pattern11/lock-status/{workflowId}")
    public ResponseEntity<Map<String, String>> p11LockStatus(@PathVariable String workflowId) {
        DistributedLockingWorkflow stub = workflowClient.newWorkflowStub(
                DistributedLockingWorkflow.class, workflowId);
        String status = stub.getLockStatus();
        return ResponseEntity.ok(Map.of("workflowId", workflowId, "lockStatus", status));
    }

    /** Query current wait status for a LockWaitWorkflow child. */
    @GetMapping("/pattern11/lock-wait-status/{workflowId}")
    public ResponseEntity<Map<String, String>> p11LockWaitStatus(@PathVariable String workflowId) {
        LockWaitWorkflow stub = workflowClient.newWorkflowStub(LockWaitWorkflow.class, workflowId);
        String status = stub.getLockWaitStatus();
        return ResponseEntity.ok(Map.of("workflowId", workflowId, "lockWaitStatus", status));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pattern 12 – File Dependency Graph
    // Scenarios:
    //   1. no-cycles      → E,A (no deps) + B→A, C→A+B, D→C — clean topo sort → ordered processing
    //   2. dependency-wait → missing upstream file triggers DependencyWaitWorkflow before processing
    //   3. cycle-resolution → artificial cycle injected, detected, broken, then processed
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Scenario 1 — No Cycles: clean A→B→C→D chain (+ independent E).
     * Flow: Parse Headers → Extract Metadata → Build Graph → Validate (no cycles) →
     *       Topological Sort → OrderedProcessingWorkflow (Ready→Processing→Completed queues).
     */
    @PostMapping("/pattern12/no-cycles")
    public ResponseEntity<Map<String, String>> p12NoCycles() {
        // fileE, fileA have no dependencies — processed first
        // fileB depends on fileA; fileC depends on fileA+fileB; fileD depends on fileC
        List<FileMetadata> files = List.of(
                buildFile("incoming/daily_trades_20260831.csv",  "fileE.csv", 0),
                buildFile("incoming/payment_batch_2026.csv",     "fileA.csv", 0),
                buildFile("incoming/malformed_batch.csv",        "fileB.csv", 0),
                buildFile("incoming/corrupted_claims.xml",       "fileC.csv", 0),
                buildFile("incoming/large_transaction_dump.dat", "fileD.csv", 0));

        String workflowId = "p12-no-cycles-" + UUID.randomUUID();
        DependencyGraphWorkflow stub = workflowClient.newWorkflowStub(
                DependencyGraphWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("dependency-graph"))
                        .build());
        WorkflowClient.start(stub::manageFileDependencies, files);
        log.info("Pattern 12 Scenario 1 (no-cycles) started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 12 Scenario 1: Clean dependency chain (E, A→B→C→D) — no cycles");
    }

    /**
     * Scenario 2 — Dependencies Missing: one file arrives before its upstream is ready.
     * DependencyWaitWorkflow polls until dependencies become available, then proceeds.
     * Flow: Graph built with missing upstream → DependencyWaitWorkflow spawned → waits →
     *       deps resolved → OrderedProcessingWorkflow.
     */
    @PostMapping("/pattern12/dependency-wait")
    public ResponseEntity<Map<String, String>> p12DependencyWait() {
        // Only B and C are submitted — A is missing; B and C will go into DependencyWaitWorkflow
        List<FileMetadata> files = List.of(
                buildFile("incoming/malformed_batch.csv",  "fileB.csv", 0)
                        .toBuilder().dependencies(List.of("fileA.csv")).build(),
                buildFile("incoming/corrupted_claims.xml", "fileC.csv", 0)
                        .toBuilder().dependencies(List.of("fileA.csv", "fileB.csv")).build());

        String workflowId = "p12-dep-wait-" + UUID.randomUUID();
        DependencyGraphWorkflow stub = workflowClient.newWorkflowStub(
                DependencyGraphWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("dependency-graph"))
                        .build());
        WorkflowClient.start(stub::manageFileDependencies, files);
        log.info("Pattern 12 Scenario 2 (dependency-wait) started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 12 Scenario 2: Missing upstream deps → DependencyWaitWorkflow → poll → process");
    }

    /**
     * Scenario 3 — Cycle Resolution: A→B→C→A forms a cycle.
     * Flow: Build Graph → Validate Cycles (cycle found) → Cycle Resolution (break edge) →
     *       Topological Sort → OrderedProcessingWorkflow.
     */
    @PostMapping("/pattern12/cycle-resolution")
    public ResponseEntity<Map<String, String>> p12CycleResolution() {
        FileMetadata cycleA = buildFile("incoming/daily_trades_20260831.csv",  "cycleA.csv", 0);
        FileMetadata cycleB = buildFile("incoming/payment_batch_2026.csv",     "cycleB.csv", 0);
        FileMetadata cycleC = buildFile("incoming/malformed_batch.csv",        "cycleC.csv", 0);
        List<FileMetadata> files = List.of(
                cycleA.toBuilder().dependencies(List.of(cycleC.getFilePath())).build(),
                cycleB.toBuilder().dependencies(List.of(cycleA.getFilePath())).build(),
                cycleC.toBuilder().dependencies(List.of(cycleB.getFilePath())).build());

        String workflowId = "p12-cycle-" + UUID.randomUUID();
        DependencyGraphWorkflow stub = workflowClient.newWorkflowStub(
                DependencyGraphWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(TemporalRuntime.queue("dependency-graph"))
                        .build());
        WorkflowClient.start(stub::manageFileDependencies, files);
        log.info("Pattern 12 Scenario 3 (cycle-resolution) started: workflowId={}", workflowId);
        return ok(workflowId, "Pattern 12 Scenario 3: Cycle A→B→C→A detected → broken → topological sort → processed");
    }

    /** Query current processing stage for a DependencyGraphWorkflow. */
    @GetMapping("/pattern12/stage/{workflowId}")
    public ResponseEntity<Map<String, String>> p12Stage(@PathVariable String workflowId) {
        DependencyGraphWorkflow stub = workflowClient.newWorkflowStub(DependencyGraphWorkflow.class, workflowId);
        return ResponseEntity.ok(Map.of(
                "workflowId", workflowId,
                "stage", stub.getProcessingStage(),
                "completedFiles", String.valueOf(stub.getCompletedFileCount())));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private ResponseEntity<Map<String, String>> ok(String workflowId, String message) {
        return ResponseEntity.ok(Map.of(
                "workflowId", workflowId,
                "message", message,
                "status", "STARTED"));
    }

    private FileMetadata defaultFile(FileMetadata provided, String defaultName) {
        if (provided != null) return provided;
        return buildFile("incoming/" + defaultName, defaultName, 0);
    }

    private long resolveSize(String relativePath) {
        var p = Paths.get(dataDir).resolve(relativePath).toAbsolutePath().normalize();
        try { if (Files.exists(p)) return Files.size(p); } catch (IOException ignored) {}
        return 0;
    }

    /** Resolves the path relative to app.data.dir, reads the real file size,
     *  computes a real SHA-256 checksum, and builds a FileMetadata that activities
     *  can work with directly. */
    private FileMetadata buildFile(String relativePath, String name, long fallbackSize) {
        var p = Paths.get(dataDir).resolve(relativePath).toAbsolutePath().normalize();
        long size = fallbackSize;
        String checksum = "";
        try {
            if (Files.exists(p)) {
                size = Files.size(p);
                checksum = sha256(p.toString());
            }
        } catch (IOException ignored) {}
        return FileMetadata.builder()
                .filePath(p.toString())
                .fileName(name)
                .fileSize(size)
                .fileChecksum(checksum)
                .fileType("BATCH")
                .arrivalTimestamp(System.currentTimeMillis())
                .sourceSystem("SFG")
                .build();
    }

    /** Compute SHA-256 hex of a file (same algorithm as FileManagementActivitiesImpl). */
    private String sha256(String filePath) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (InputStream is = Files.newInputStream(Paths.get(filePath))) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = is.read(buf)) != -1) md.update(buf, 0, n);
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (Exception e) {
            log.warn("SHA-256 computation failed for {}: {}", filePath, e.getMessage());
            return "";
        }
    }
}
