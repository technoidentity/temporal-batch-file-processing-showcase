package com.example.temporalshowcase.worker;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.*;
import com.example.temporalshowcase.workflows.pattern1.FileProcessingWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern2.LargeFileProcessingWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern2.ProcessChunkWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern3.CircuitBreakerWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern4.CorruptedFileSagaWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern5.IdempotentFileProcessingWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern6.*;
import com.example.temporalshowcase.workflows.pattern7.PermissionEscalationWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern8.NormalTransferWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern8.ResumableTransferWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern9.NormalProcessingWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern9.RateLimitingWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern10.MultiStageValidationWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern11.DistributedLockingWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern11.LockWaitWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern12.DependencyGraphWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern12.OrderedProcessingWorkflowImpl;
import com.example.temporalshowcase.workflows.pattern12.DependencyWaitWorkflowImpl;
import io.temporal.api.workflowservice.v1.GetSystemInfoRequest;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

/**
 * Registers all workflow implementations and activity implementations with
 * their corresponding Temporal task queues.  Each pattern gets its own
 * task queue for isolation and independent scaling.
 */
@Slf4j
@Component
@DependsOn("temporalRuntime") // ensure TemporalRuntime's static config is populated before workers register
@RequiredArgsConstructor
public class ShowcaseWorker {

    private final WorkerFactory workerFactory;
    private final WorkflowServiceStubs workflowServiceStubs;

    // Activity implementations injected by Spring
    private final FileManagementActivitiesImpl fileManagementActivities;
    private final CircuitBreakerActivitiesImpl circuitBreakerActivities;
    private final SagaActivitiesImpl sagaActivities;
    private final IdempotencyActivitiesImpl idempotencyActivities;
    private final FormatDetectionActivitiesImpl formatDetectionActivities;
    private final PermissionActivitiesImpl permissionActivities;
    private final TransferActivitiesImpl transferActivities;
    private final ResourceMonitorActivitiesImpl resourceMonitorActivities;
    private final ValidationActivitiesImpl validationActivities;
    private final LockActivitiesImpl lockActivities;
    private final DependencyActivitiesImpl dependencyActivities;

    @PostConstruct
    public void registerAndStart() {
        // ── Pattern 1: Signal-based File Arrival ─────────────────────────────
        Worker p1Worker = workerFactory.newWorker(TemporalRuntime.queue("delayed-file"));
        p1Worker.registerWorkflowImplementationTypes(FileProcessingWorkflowImpl.class);
        p1Worker.registerActivitiesImplementations(fileManagementActivities);
        log.info("Registered worker for queue: {}", TemporalRuntime.queue("delayed-file"));

        // ── Pattern 2: Large File Chunked Processing ─────────────────────────
        Worker p2ParentWorker = workerFactory.newWorker(TemporalRuntime.queue("large-file"));
        p2ParentWorker.registerWorkflowImplementationTypes(LargeFileProcessingWorkflowImpl.class);
        p2ParentWorker.registerActivitiesImplementations(fileManagementActivities);

        Worker p2ChunkWorker = workerFactory.newWorker(TemporalRuntime.queue("chunk-processing"));
        p2ChunkWorker.registerWorkflowImplementationTypes(ProcessChunkWorkflowImpl.class);
        p2ChunkWorker.registerActivitiesImplementations(fileManagementActivities);

        // Retry queue — handles failed chunk downloads
        Worker p2RetryWorker = workerFactory.newWorker(TemporalRuntime.queue("chunk-retry"));
        p2RetryWorker.registerWorkflowImplementationTypes(ProcessChunkWorkflowImpl.class);
        p2RetryWorker.registerActivitiesImplementations(fileManagementActivities);
        log.info("Registered workers for queues: {}, {}, {}",
                TemporalRuntime.queue("large-file"), TemporalRuntime.queue("chunk-processing"),
                TemporalRuntime.queue("chunk-retry"));

        // ── Pattern 3: Circuit Breaker ────────────────────────────────────────
        Worker p3Worker = workerFactory.newWorker(TemporalRuntime.queue("circuit-breaker"));
        p3Worker.registerWorkflowImplementationTypes(CircuitBreakerWorkflowImpl.class);
        p3Worker.registerActivitiesImplementations(fileManagementActivities, circuitBreakerActivities);
        log.info("Registered worker for queue: {}", TemporalRuntime.queue("circuit-breaker"));

        // ── Pattern 4: Saga Recovery ──────────────────────────────────────────
        Worker p4Worker = workerFactory.newWorker(TemporalRuntime.queue("saga-recovery"));
        p4Worker.registerWorkflowImplementationTypes(CorruptedFileSagaWorkflowImpl.class);
        p4Worker.registerActivitiesImplementations(sagaActivities, fileManagementActivities);
        log.info("Registered worker for queue: {}", TemporalRuntime.queue("saga-recovery"));

        // ── Pattern 5: Idempotency ────────────────────────────────────────────
        Worker p5Worker = workerFactory.newWorker(TemporalRuntime.queue("idempotency"));
        p5Worker.registerWorkflowImplementationTypes(
                IdempotentFileProcessingWorkflowImpl.class,
                FileProcessingWorkflowImpl.class);   // child workflow on same queue
        p5Worker.registerActivitiesImplementations(fileManagementActivities, idempotencyActivities);
        log.info("Registered worker for queue: {}", TemporalRuntime.queue("idempotency"));

        // ── Pattern 6: Multi-Format Handler ──────────────────────────────────
        Worker p6Worker = workerFactory.newWorker(TemporalRuntime.queue("multi-format"));
        p6Worker.registerWorkflowImplementationTypes(
                MultiFormatHandlerWorkflowImpl.class,
                CsvProcessorWorkflowImpl.class,
                XmlProcessorWorkflowImpl.class,
                JsonProcessorWorkflowImpl.class,
                FixedWidthProcessorWorkflowImpl.class);
        p6Worker.registerActivitiesImplementations(formatDetectionActivities);
        log.info("Registered worker for queue: {}", TemporalRuntime.queue("multi-format"));

        // ── Pattern 7: Permission Escalation ─────────────────────────────────
        Worker p7Worker = workerFactory.newWorker(TemporalRuntime.queue("permission-escalation"));
        p7Worker.registerWorkflowImplementationTypes(PermissionEscalationWorkflowImpl.class);
        p7Worker.registerActivitiesImplementations(permissionActivities);
        log.info("Registered worker for queue: {}", TemporalRuntime.queue("permission-escalation"));

        // ── Pattern 8: Resumable Transfer ────────────────────────────────────
        Worker p8Worker = workerFactory.newWorker(TemporalRuntime.queue("resumable-transfer"));
        p8Worker.registerWorkflowImplementationTypes(ResumableTransferWorkflowImpl.class, NormalTransferWorkflowImpl.class);
        p8Worker.registerActivitiesImplementations(transferActivities);
        log.info("Registered worker for queue: {}", TemporalRuntime.queue("resumable-transfer"));

        // ── Pattern 9: Rate Limiting ──────────────────────────────────────────
        Worker p9Worker = workerFactory.newWorker(TemporalRuntime.queue("rate-limiting"));
        p9Worker.registerWorkflowImplementationTypes(RateLimitingWorkflowImpl.class, NormalProcessingWorkflowImpl.class);
        p9Worker.registerActivitiesImplementations(resourceMonitorActivities);
        log.info("Registered worker for queue: {}", TemporalRuntime.queue("rate-limiting"));

        // ── Pattern 10: Multi-Stage Validation ───────────────────────────────
        Worker p10Worker = workerFactory.newWorker(TemporalRuntime.queue("validation"));
        p10Worker.registerWorkflowImplementationTypes(MultiStageValidationWorkflowImpl.class);
        p10Worker.registerActivitiesImplementations(validationActivities);
        log.info("Registered worker for queue: {}", TemporalRuntime.queue("validation"));

        // ── Pattern 11: Distributed Locking ──────────────────────────────────
        Worker p11Worker = workerFactory.newWorker(TemporalRuntime.queue("locking"));
        p11Worker.registerWorkflowImplementationTypes(DistributedLockingWorkflowImpl.class, LockWaitWorkflowImpl.class);
        p11Worker.registerActivitiesImplementations(lockActivities, fileManagementActivities);
        log.info("Registered worker for queue: {}", TemporalRuntime.queue("locking"));

        // ── Pattern 12: Dependency Graph ─────────────────────────────────────
        Worker p12Worker = workerFactory.newWorker(TemporalRuntime.queue("dependency-graph"));
        p12Worker.registerWorkflowImplementationTypes(
                DependencyGraphWorkflowImpl.class,
                OrderedProcessingWorkflowImpl.class,
                DependencyWaitWorkflowImpl.class);
        p12Worker.registerActivitiesImplementations(dependencyActivities, fileManagementActivities);
        log.info("Registered worker for queue: {}", TemporalRuntime.queue("dependency-graph"));

        // Start the factory off the startup thread so the app (and REST API) come
        // up even when Temporal is unreachable. The workers begin polling as soon
        // as Temporal becomes available.
        Thread starter = new Thread(this::startWhenReachable, "temporal-worker-starter");
        starter.setDaemon(true);
        starter.start();
    }

    private void startWhenReachable() {
        int attempt = 0;
        while (!temporalReachable()) {
            attempt++;
            log.warn("Temporal not reachable yet (attempt {}) — workers registered, retrying start in 10s", attempt);
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        workerFactory.start();
        log.info("All Temporal workers started successfully.");
    }

    private boolean temporalReachable() {
        try {
            workflowServiceStubs.blockingStub().getSystemInfo(GetSystemInfoRequest.newBuilder().build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
