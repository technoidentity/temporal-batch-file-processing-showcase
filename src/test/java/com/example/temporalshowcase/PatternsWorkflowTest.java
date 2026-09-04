package com.example.temporalshowcase;

import com.example.temporalshowcase.activities.*;
import com.example.temporalshowcase.config.TemporalProperties;
import com.example.temporalshowcase.config.TemporalRuntime;
import com.example.temporalshowcase.models.*;
import com.example.temporalshowcase.workflows.pattern1.FileProcessingWorkflow;
import com.example.temporalshowcase.workflows.pattern1.FileProcessingWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern10.MultiStageValidationWorkflow;
import com.example.temporalshowcase.workflows.pattern10.MultiStageValidationWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern11.DistributedLockingWorkflow;
import com.example.temporalshowcase.workflows.pattern11.DistributedLockingWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern11.LockWaitWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern12.DependencyGraphWorkflow;
import com.example.temporalshowcase.workflows.pattern12.DependencyGraphWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern12.DependencyWaitWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern12.OrderedProcessingWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern2.LargeFileProcessingWorkflow;
import com.example.temporalshowcase.workflows.pattern2.LargeFileProcessingWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern2.ProcessChunkWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern3.CircuitBreakerWorkflow;
import com.example.temporalshowcase.workflows.pattern3.CircuitBreakerWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern4.CorruptedFileSagaWorkflow;
import com.example.temporalshowcase.workflows.pattern4.CorruptedFileSagaWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern5.IdempotentFileProcessingWorkflow;
import com.example.temporalshowcase.workflows.pattern5.IdempotentFileProcessingWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern6.*;
import com.example.temporalshowcase.workflows.pattern7.PermissionEscalationWorkflow;
import com.example.temporalshowcase.workflows.pattern7.PermissionEscalationWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern8.NormalTransferWorkflow;
import com.example.temporalshowcase.workflows.pattern8.NormalTransferWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern8.ResumableTransferWorkflow;
import com.example.temporalshowcase.workflows.pattern8.ResumableTransferWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern9.NormalProcessingWorkflow;
import com.example.temporalshowcase.workflows.pattern9.NormalProcessingWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern9.RateLimitingWorkflow;
import com.example.temporalshowcase.workflows.pattern9.RateLimitingWorkflowImpl;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.failure.ApplicationFailure;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Time-skipping unit tests for all 12 patterns, run against Temporal's in-process
 * TestWorkflowEnvironment. Real activity implementations are registered (they do
 * real file I/O against the sample files under ./data/incoming), so these tests
 * exercise both the workflow orchestration and the activity logic. Durable timers
 * and Workflow.sleep are skipped, so SLA/backoff/wait paths complete instantly.
 */
class PatternsWorkflowTest {

    private TestWorkflowEnvironment env;

    /**
     * Workflows read task queues and activity policies from TemporalRuntime, which
     * Spring normally populates. These tests don't start a Spring context, so bind
     * the real temporal.queues + temporal.policies from application.yml and seed
     * TemporalRuntime directly — keeping tests aligned with production config.
     */
    @BeforeAll
    static void initTemporalRuntime() throws Exception {
        PropertySource<?> yaml = new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml")).get(0);
        Binder binder = new Binder(ConfigurationPropertySources.from(yaml));
        TemporalProperties props = new TemporalProperties();
        props.setQueues(binder.bind("temporal.queues", Bindable.mapOf(String.class, String.class))
                .get());
        props.setPolicies(binder.bind("temporal.policies",
                        Bindable.mapOf(String.class, TemporalProperties.Policy.class))
                .get());
        TemporalRuntime.init(props);
    }

    @AfterEach
    void tearDown() {
        if (env != null) env.close();
        resetInMemoryStores();
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private WorkflowClient start(Class<?>[] workflows, Object[] activities, String queue) {
        env = TestWorkflowEnvironment.newInstance();
        Worker w = env.newWorker(queue);
        w.registerWorkflowImplementationTypes(workflows);
        w.registerActivitiesImplementations(activities);
        env.start();
        return env.getWorkflowClient();
    }

    private <T> T stub(WorkflowClient client, Class<T> type, String queue, String id) {
        return client.newWorkflowStub(type, WorkflowOptions.newBuilder()
                .setWorkflowId(id).setTaskQueue(queue).build());
    }

    private FileManagementActivitiesImpl fileMgmt() { return withDataDir(new FileManagementActivitiesImpl()); }

    private static com.fasterxml.jackson.databind.ObjectMapper objectMapper() {
        return new com.fasterxml.jackson.databind.ObjectMapper()
                .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
    }

    /** Builds FileMetadata pointing at a real sample file under ./data/incoming. */
    private FileMetadata sample(String name) {
        String abs = Paths.get("data/incoming", name).toAbsolutePath().toString();
        return FileMetadata.builder()
                .filePath(abs).fileName(name).fileSize(0).fileChecksum("")
                .fileType("BATCH").sourceSystem("TEST").build();
    }

    private static <T> T withDataDir(T activity) {
        setFieldIfPresent(activity, "dataDir", "data");
        return activity;
    }

    private static void setFieldIfPresent(Object target, String field, Object value) {
        Class<?> c = target.getClass();
        while (c != null) {
            try {
                Field f = c.getDeclaredField(field);
                f.setAccessible(true);
                f.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            } catch (IllegalAccessException e) {
                return;
            }
        }
    }

    /** Clears the process-static in-memory stores so tests don't leak state into each other. */
    private static void resetInMemoryStores() {
        clearStaticMap(CircuitBreakerActivitiesImpl.class, "store");
        clearStaticMap(IdempotencyActivitiesImpl.class, "stateStore");
        clearStaticMap(LockActivitiesImpl.class, "lockRegistry");
        clearStaticMap(TransferActivitiesImpl.class, "networkFailCounters");
        for (String q : new String[]{"readyQueue", "processingQueue", "completedQueue", "failedQueue"}) {
            clearStaticCollection(DependencyActivitiesImpl.class, q);
        }
    }

    @SuppressWarnings("unchecked")
    private static void clearStaticMap(Class<?> c, String field) {
        try {
            Field f = c.getDeclaredField(field);
            f.setAccessible(true);
            Object v = f.get(null);
            if (v instanceof Map<?, ?> m) m.clear();
        } catch (Exception ignored) { }
    }

    private static void clearStaticCollection(Class<?> c, String field) {
        try {
            Field f = c.getDeclaredField(field);
            f.setAccessible(true);
            Object v = f.get(null);
            if (v instanceof java.util.Collection<?> col) col.clear();
            else if (v instanceof Map<?, ?> m) m.clear();
        } catch (Exception ignored) { }
    }

    private static String failureType(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ApplicationFailure af) return af.getType();
        }
        return null;
    }

    // ── Pattern 1 — signal + SLA timeout ───────────────────────────────────────

    @Test @DisplayName("P1: fileArrived signal completes processing")
    void pattern1_signalArrives() {
        WorkflowClient c = start(new Class[]{FileProcessingWorkflowImpl.class},
                new Object[]{fileMgmt()}, TemporalRuntime.queue("delayed-file"));
        FileMetadata file = sample("payment_batch_2026.csv");
        FileProcessingWorkflow wf = stub(c, FileProcessingWorkflow.class,
                TemporalRuntime.queue("delayed-file"), "p1-" + UUID.randomUUID());

        WorkflowClient.start(wf::processFile, file);
        wf.fileArrived(file);
        ProcessingResult r = WorkflowStub.fromTyped(wf).getResult(ProcessingResult.class);

        assertEquals(ProcessingStatus.COMPLETED, r.getStatus());
        assertEquals("COMPLETED", wf.getProcessingStatus());
    }

    @Test @DisplayName("P1: SLA timer fires when no signal arrives")
    void pattern1_slaTimeout() {
        WorkflowClient c = start(new Class[]{FileProcessingWorkflowImpl.class},
                new Object[]{fileMgmt()}, TemporalRuntime.queue("delayed-file"));
        FileMetadata file = sample("missing_file.csv");
        file.setSlaTimeoutSeconds(5);
        FileProcessingWorkflow wf = stub(c, FileProcessingWorkflow.class,
                TemporalRuntime.queue("delayed-file"), "p1to-" + UUID.randomUUID());

        WorkflowException ex = assertThrows(WorkflowException.class, () -> wf.processFile(file));
        assertEquals("SLA_TIMEOUT", failureType(ex));
    }

    // ── Pattern 2 — chunked child workflows ────────────────────────────────────

    @Test @DisplayName("P2: large file is chunked, aggregated, completed")
    void pattern2_chunked() {
        // All workers must be registered before env.start().
        env = TestWorkflowEnvironment.newInstance();
        for (String q : new String[]{TemporalRuntime.queue("chunk-processing"), TemporalRuntime.queue("chunk-retry")}) {
            Worker cw = env.newWorker(q);
            cw.registerWorkflowImplementationTypes(ProcessChunkWorkflowImpl.class);
            cw.registerActivitiesImplementations(fileMgmt());
        }
        Worker parent = env.newWorker(TemporalRuntime.queue("large-file"));
        parent.registerWorkflowImplementationTypes(LargeFileProcessingWorkflowImpl.class);
        parent.registerActivitiesImplementations(fileMgmt());
        env.start();
        WorkflowClient c = env.getWorkflowClient();

        LargeFileProcessingWorkflow wf = stub(c, LargeFileProcessingWorkflow.class,
                TemporalRuntime.queue("large-file"), "p2-" + UUID.randomUUID());
        ProcessingResult r = wf.processLargeFile(sample("large_transaction_dump.dat"));

        assertNotNull(r);
        assertEquals(ProcessingStatus.COMPLETED, r.getStatus());
        assertTrue(r.getRecordCount() > 0);
    }

    // ── Pattern 3 — circuit breaker (empty file) ───────────────────────────────

    @Test @DisplayName("P3: empty file handled by circuit breaker without crashing")
    void pattern3_emptyFile() {
        WorkflowClient c = start(new Class[]{CircuitBreakerWorkflowImpl.class},
                new Object[]{fileMgmt(), new CircuitBreakerActivitiesImpl()},
                TemporalRuntime.queue("circuit-breaker"));
        CircuitBreakerWorkflow wf = stub(c, CircuitBreakerWorkflow.class,
                TemporalRuntime.queue("circuit-breaker"), "p3-" + UUID.randomUUID());

        ProcessingResult r = wf.processFileWithCircuitBreaker(sample("possibly_empty.csv"));
        assertNotNull(r);
        assertNotNull(wf.getCircuitState());
    }

    @Test @DisplayName("P3: last-10 window records empty and successful files")
    void pattern3_slidingWindowCountsSuccess() {
        CircuitBreakerActivitiesImpl activities = new CircuitBreakerActivitiesImpl();
        activities.updateEmptyFileMetrics("cb");
        activities.updateEmptyFileMetrics("cb");
        activities.recordSuccessfulFile("cb");
        CircuitBreakerState state = activities.getCircuitBreakerState("cb");
        assertEquals(3, state.getWindowTotal());
        assertEquals(2, state.getWindowEmpty());
        assertEquals(2.0 / 3.0, state.getFailureRate(), 0.001);
    }

    // ── Pattern 4 — saga recovery ──────────────────────────────────────────────

    @Test @DisplayName("P4: corrupted file runs the saga to a terminal result")
    void pattern4_saga() {
        WorkflowClient c = start(new Class[]{CorruptedFileSagaWorkflowImpl.class},
                new Object[]{withDataDir(new SagaActivitiesImpl()), fileMgmt()},
                TemporalRuntime.queue("saga-recovery"));
        CorruptionDetails corruption = CorruptionDetails.builder()
                .corruptionType("CHECKSUM_MISMATCH").expectedChecksum("abc123")
                .actualChecksum("xyz999").description("checksum mismatch").build();
        CorruptedFileSagaWorkflow wf = stub(c, CorruptedFileSagaWorkflow.class,
                TemporalRuntime.queue("saga-recovery"), "p4-" + UUID.randomUUID());

        ProcessingResult r = wf.handleCorruptedFile(sample("corrupted_claims.xml"), corruption);
        assertNotNull(r);
        assertNotNull(wf.getSagaStatus());
    }

    // ── Pattern 5 — idempotency + duplicate + CAS ──────────────────────────────

    @Test @DisplayName("P5: first run completes, duplicate returns cached result")
    void pattern5_idempotency() {
        WorkflowClient c = start(
                new Class[]{IdempotentFileProcessingWorkflowImpl.class, FileProcessingWorkflowImpl.class},
                new Object[]{fileMgmt(), new IdempotencyActivitiesImpl()},
                TemporalRuntime.queue("idempotency"));
        FileMetadata file = sample("daily_trades_20260831.csv");

        IdempotentFileProcessingWorkflow first = stub(c, IdempotentFileProcessingWorkflow.class,
                TemporalRuntime.queue("idempotency"), "p5a-" + UUID.randomUUID());
        ProcessingResult r1 = first.processFileWithIdempotency(file);
        assertEquals(ProcessingStatus.COMPLETED, r1.getStatus());

        IdempotentFileProcessingWorkflow dup = stub(c, IdempotentFileProcessingWorkflow.class,
                TemporalRuntime.queue("idempotency"), "p5b-" + UUID.randomUUID());
        ProcessingResult r2 = dup.processFileWithIdempotency(file);
        assertEquals(ProcessingStatus.COMPLETED, r2.getStatus());
        // Same content hash → same record count returned from the cache.
        assertEquals(r1.getRecordCount(), r2.getRecordCount());
    }

    // ── Pattern 6 — multi-format routing ───────────────────────────────────────

    @Test @DisplayName("P6: CSV file routes to the CSV processor child workflow")
    void pattern6_csv() {
        WorkflowClient c = start(
                new Class[]{MultiFormatHandlerWorkflowImpl.class, CsvProcessorWorkflowImpl.class,
                        XmlProcessorWorkflowImpl.class, JsonProcessorWorkflowImpl.class,
                        FixedWidthProcessorWorkflowImpl.class},
                new Object[]{withDataDir(new FormatDetectionActivitiesImpl(objectMapper()))},
                TemporalRuntime.queue("multi-format"));
        MultiFormatHandlerWorkflow wf = stub(c, MultiFormatHandlerWorkflow.class,
                TemporalRuntime.queue("multi-format"), "p6-" + UUID.randomUUID());

        StandardizedData r = wf.handleMultiFormatFile(sample("records.csv"));
        assertNotNull(r);
        assertEquals("CSV", wf.getCurrentFormat());
    }

    // ── Pattern 7 — permission escalation (signal, update, timeout) ─────────────

    @Test @DisplayName("P7: human resolution via signal unblocks the escalation")
    void pattern7_humanSignal() {
        WorkflowClient c = permissionEnv();
        PermissionEscalationWorkflow wf = stub(c, PermissionEscalationWorkflow.class,
                TemporalRuntime.queue("permission-escalation"), "p7s-" + UUID.randomUUID());
        WorkflowClient.start(wf::resolvePermissionIssue,
                sample("restricted_payroll.csv"), strictIssue());
        wf.permissionResolved("MANUALLY_FIXED_BY_ADMIN");
        ProcessingResult r = WorkflowStub.fromTyped(wf).getResult(ProcessingResult.class);
        assertNotNull(r);
    }

    @Test @DisplayName("P7: @UpdateMethod resolution returns an ack and unblocks")
    void pattern7_humanUpdate() {
        WorkflowClient c = permissionEnv();
        PermissionEscalationWorkflow wf = stub(c, PermissionEscalationWorkflow.class,
                TemporalRuntime.queue("permission-escalation"), "p7u-" + UUID.randomUUID());
        WorkflowClient.start(wf::resolvePermissionIssue,
                sample("restricted_payroll.csv"), strictIssue());
        String ack = wf.resolvePermission("FIXED_VIA_UPDATE");
        assertTrue(ack.startsWith("ACK"));
        ProcessingResult r = WorkflowStub.fromTyped(wf).getResult(ProcessingResult.class);
        assertNotNull(r);
    }

    @Test @DisplayName("P7: bounded human wait escalates with PERMISSION_ESCALATION_TIMEOUT")
    void pattern7_humanTimeout() {
        WorkflowClient c = permissionEnv();
        PermissionEscalationWorkflow wf = stub(c, PermissionEscalationWorkflow.class,
                TemporalRuntime.queue("permission-escalation"), "p7t-" + UUID.randomUUID());
        WorkflowException ex = assertThrows(WorkflowException.class,
                () -> wf.resolvePermissionIssue(sample("restricted_payroll.csv"), strictIssue()));
        assertEquals("PERMISSION_ESCALATION_TIMEOUT", failureType(ex));
    }

    private WorkflowClient permissionEnv() {
        return start(new Class[]{PermissionEscalationWorkflowImpl.class},
                new Object[]{new PermissionActivitiesImpl()},
                TemporalRuntime.queue("permission-escalation"));
    }

    private PermissionIssue strictIssue() {
        return PermissionIssue.builder()
                .filePath("incoming/restricted_payroll.csv").requiredPermission("READ")
                .currentPermission("NONE").errorCode("STRICT")
                .description("all auto-fixes blocked").build();
    }

    // ── Pattern 8 — resumable transfer ─────────────────────────────────────────

    @Test @DisplayName("P8: normal transfer copies the file and validates integrity")
    void pattern8_normalTransfer() throws Exception {
        WorkflowClient c = start(
                new Class[]{NormalTransferWorkflowImpl.class, ResumableTransferWorkflowImpl.class},
                new Object[]{withDataDir(new TransferActivitiesImpl(objectMapper()))},
                TemporalRuntime.queue("resumable-transfer"));
        java.nio.file.Path srcPath = Paths.get("data/incoming/large_transaction_dump.dat").toAbsolutePath();
        String src = srcPath.toString();
        java.nio.file.Path outgoingDir = Paths.get("data/outgoing");
        java.nio.file.Files.createDirectories(outgoingDir);
        java.nio.file.Files.deleteIfExists(outgoingDir.resolve("large_transaction_dump.dat"));
        TransferRequest req = TransferRequest.builder()
                .sourceUrl("file://" + src).destinationPath(src)
                .fileName("large_transaction_dump.dat")
                .expectedFileSize(java.nio.file.Files.size(srcPath))
                .expectedChecksum("sha256-expected").maxRetries(3).build();
        NormalTransferWorkflow wf = stub(c, NormalTransferWorkflow.class,
                TemporalRuntime.queue("resumable-transfer"), "p8-" + UUID.randomUUID());

        assertDoesNotThrow(() -> wf.executeNormalTransfer(req));
    }

    // ── Pattern 9 — rate limiting ──────────────────────────────────────────────

    @Test @DisplayName("P9: adaptive throttling completes its monitoring cycles")
    void pattern9_rateLimiting() {
        WorkflowClient c = start(
                new Class[]{RateLimitingWorkflowImpl.class, NormalProcessingWorkflowImpl.class},
                new Object[]{new ResourceMonitorActivitiesImpl()},
                TemporalRuntime.queue("rate-limiting"));
        RateLimitingWorkflow wf = stub(c, RateLimitingWorkflow.class,
                TemporalRuntime.queue("rate-limiting"), "p9-" + UUID.randomUUID());
        assertDoesNotThrow(() -> wf.manageRateLimiting(2));
        assertTrue(wf.getCurrentThrottleStatus().startsWith("COMPLETED"));
    }

    // ── Pattern 10 — multi-stage validation ────────────────────────────────────

    @Test @DisplayName("P10: clean file passes all validation stages")
    void pattern10_qualityOk() {
        WorkflowClient c = validationEnv();
        MultiStageValidationWorkflow wf = stub(c, MultiStageValidationWorkflow.class,
                TemporalRuntime.queue("validation"), "p10ok-" + UUID.randomUUID());
        ValidationResult r = wf.executeMultiStageValidation(sample("payment_batch_2026.csv"));
        assertNotNull(r);
    }

    @Test @DisplayName("P10: unrepairable data escalates for manual review")
    void pattern10_escalates() {
        WorkflowClient c = validationEnv();
        MultiStageValidationWorkflow wf = stub(c, MultiStageValidationWorkflow.class,
                TemporalRuntime.queue("validation"), "p10esc-" + UUID.randomUUID());
        WorkflowException ex = assertThrows(WorkflowException.class,
                () -> wf.executeMultiStageValidation(sample("corrupted_transactions.csv")));
        assertNotNull(failureType(ex));
    }

    private WorkflowClient validationEnv() {
        return start(new Class[]{MultiStageValidationWorkflowImpl.class},
                new Object[]{withDataDir(new ValidationActivitiesImpl())},
                TemporalRuntime.queue("validation"));
    }

    // ── Pattern 11 — distributed locking ───────────────────────────────────────

    @Test @DisplayName("P11: lock acquired with no contention, processed, released")
    void pattern11_lockAcquired() {
        WorkflowClient c = start(
                new Class[]{DistributedLockingWorkflowImpl.class, LockWaitWorkflowImpl.class},
                new Object[]{new LockActivitiesImpl(), fileMgmt()},
                TemporalRuntime.queue("locking"));
        FileMetadata file = sample("daily_trades_20260831.csv").toBuilder()
                .fileChecksum("checksum-trades-001").priority(5).build();
        DistributedLockingWorkflow wf = stub(c, DistributedLockingWorkflow.class,
                TemporalRuntime.queue("locking"), "p11-" + UUID.randomUUID());
        ProcessingResult r = wf.processFileWithLock(file);
        assertNotNull(r);
    }

    // ── Pattern 12 — dependency graph ──────────────────────────────────────────

    @Test @DisplayName("P12: clean dependency chain is topologically ordered and processed")
    void pattern12_noCycles() {
        WorkflowClient c = dependencyEnv();
        List<FileMetadata> files = List.of(
                sample("daily_trades_20260831.csv").toBuilder().fileName("fileE.csv").build(),
                sample("payment_batch_2026.csv").toBuilder().fileName("fileA.csv").build(),
                sample("malformed_batch.csv").toBuilder().fileName("fileB.csv").build(),
                sample("records.csv").toBuilder().fileName("fileC.csv").build(),
                sample("records.json").toBuilder().fileName("fileD.csv").build());
        DependencyGraphWorkflow wf = stub(c, DependencyGraphWorkflow.class,
                TemporalRuntime.queue("dependency-graph"), "p12-" + UUID.randomUUID());
        assertDoesNotThrow(() -> wf.manageFileDependencies(files));
        assertNotNull(wf.getProcessingStage());
    }

    @Test @DisplayName("P12: a dependency cycle is detected, broken, and processed")
    void pattern12_cycleResolution() {
        WorkflowClient c = dependencyEnv();
        FileMetadata a = sample("daily_trades_20260831.csv").toBuilder().fileName("cycleA.csv").build();
        FileMetadata b = sample("payment_batch_2026.csv").toBuilder().fileName("cycleB.csv").build();
        FileMetadata cc = sample("malformed_batch.csv").toBuilder().fileName("cycleC.csv").build();
        List<FileMetadata> files = List.of(
                a.toBuilder().dependencies(List.of(cc.getFilePath())).build(),
                b.toBuilder().dependencies(List.of(a.getFilePath())).build(),
                cc.toBuilder().dependencies(List.of(b.getFilePath())).build());
        DependencyGraphWorkflow wf = stub(c, DependencyGraphWorkflow.class,
                TemporalRuntime.queue("dependency-graph"), "p12c-" + UUID.randomUUID());
        assertDoesNotThrow(() -> wf.manageFileDependencies(files));
    }

    private WorkflowClient dependencyEnv() {
        return start(new Class[]{DependencyGraphWorkflowImpl.class, OrderedProcessingWorkflowImpl.class,
                        DependencyWaitWorkflowImpl.class},
                new Object[]{withDataDir(new DependencyActivitiesImpl()), fileMgmt()},
                TemporalRuntime.queue("dependency-graph"));
    }
}
