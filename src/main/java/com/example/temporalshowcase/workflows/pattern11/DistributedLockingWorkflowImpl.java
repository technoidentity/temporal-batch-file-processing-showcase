package com.example.temporalshowcase.workflows.pattern11;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.FileManagementActivities;
import com.example.temporalshowcase.activities.LockActivities;
import com.example.temporalshowcase.models.*;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Workflow;

import java.time.Duration;

/**
 * Pattern 11: Distributed Locking implementation.
 *
 * Lock Acquired path:
 *  1. Acquire File Lock / Distributed lock
 *  2. Process File / Exclusive access
 *  3. Release Lock / Cleanup
 *  4. Notify Waiting Processes
 *
 * Lock Denied path:
 *  → Spawns a child LockWaitWorkflow which polls, backs off, and either
 *    acquires the lock or escalates on timeout.
 *
 * Lock Management:
 *  - Lock Registry Service  : in-memory ConcurrentHashMap in LockActivitiesImpl
 *  - Lock Timeout Manager   : acquireLock checks expiresAt; forceReleaseLock for expired locks
 *  - Lock Owner Tracker     : getLockInfo / isWorkflowActive
 *  - Lock Conflict Resolver : forceReleaseLock on dead holder, priority + FCFS resolution
 *
 * Conflict Resolution:
 *  - Priority-Based Resolution    : resolveByPriority()
 *  - First-Come-First-Serve Policy: resolveByFCFS()
 *  - Timeout-Based Release        : timeoutBasedRelease()
 *  - Manual Intervention Override : manualInterventionOverride() (available via activity)
 */
public class DistributedLockingWorkflowImpl implements DistributedLockingWorkflow {

    private final Duration LOCK_TIMEOUT      = TemporalRuntime.patterns().getLocking().getLockTimeout();
    private final int      MAX_LOCK_ATTEMPTS = TemporalRuntime.patterns().getLocking().getMaxLockAttempts();

    private String lockStatus = "STARTING";

    private final LockActivities lockActivities = Workflow.newActivityStub(
            LockActivities.class,
            TemporalRuntime.activity("lock-acquire"));

    private final FileManagementActivities fileActivities = Workflow.newActivityStub(
            FileManagementActivities.class,
            TemporalRuntime.activity("file-long"));

    @Override
    public ProcessingResult processFileWithLock(FileMetadata fileMetadata) {
        String lockKey = buildLockKey(fileMetadata);
        LockHandle lockHandle = acquireLockWithWaitWorkflow(lockKey, fileMetadata);

        try {
            lockStatus = "PROCESSING";
            lockActivities.logLockEvent(lockKey, "LOCK_ACQUIRED", Workflow.getInfo().getWorkflowId());
            if (fileMetadata.getHoldLockSeconds() > 0) {
                lockStatus = "HOLDING_LOCK " + fileMetadata.getHoldLockSeconds() + "s";
                Workflow.sleep(Duration.ofSeconds(fileMetadata.getHoldLockSeconds()));
            }
            ProcessingResult result = fileActivities.processFileContent(fileMetadata);
            lockActivities.logLockEvent(lockKey, "PROCESSING_COMPLETE", Workflow.getInfo().getWorkflowId());
            return result;
        } finally {
            lockStatus = "RELEASING";
            releaseLock(lockHandle);
            lockActivities.notifyWaitingProcesses(lockKey);
            lockStatus = "COMPLETED";
        }
    }

    /**
     * Tries a fast local acquire first (Lock Acquired path).
     * If denied, delegates to {@link LockWaitWorkflow} (Lock Denied path).
     */
    private LockHandle acquireLockWithWaitWorkflow(String lockKey, FileMetadata fileMetadata) {
        lockStatus = "ACQUIRING";

        // Quick attempt — may succeed immediately (Scenario 1: no contention)
        LockResult quickResult = lockActivities.acquireLock(
                LockRequest.builder()
                        .lockKey(lockKey)
                        .lockTimeout(LOCK_TIMEOUT)
                        .requesterWorkflowId(Workflow.getInfo().getWorkflowId())
                        .priority(fileMetadata.getPriority())
                        .build());

        if (quickResult.isSuccess()) {
            lockStatus = "LOCK_ACQUIRED_IMMEDIATELY";
            return quickResult.getLockHandle();
        }

        // ── Lock Denied → delegate to LockWaitWorkflow ──
        lockStatus = "LOCK_DENIED — delegating to LockWaitWorkflow";
        lockActivities.logLockEvent(lockKey, "LOCK_DENIED_WAITING", Workflow.getInfo().getWorkflowId());

        LockWaitWorkflow waitWorkflow = Workflow.newChildWorkflowStub(
                LockWaitWorkflow.class,
                io.temporal.workflow.ChildWorkflowOptions.newBuilder()
                        .setWorkflowId("lock-wait-" + Workflow.getInfo().getWorkflowId())
                        .setTaskQueue(TemporalRuntime.queue("locking"))
                        .build());

        LockHandle acquired = waitWorkflow.waitForLock(
                lockKey,
                Workflow.getInfo().getWorkflowId(),
                fileMetadata.getPriority());

        lockStatus = "LOCK_ACQUIRED_AFTER_WAIT";
        return acquired;
    }

    /** Releases the lock and logs the event; swallows exceptions so the finally block always completes. */
    private void releaseLock(LockHandle lockHandle) {
        try {
            lockActivities.releaseLock(lockHandle);
            lockActivities.logLockEvent(lockHandle.getLockKey(), "LOCK_RELEASED", lockHandle.getOwnerWorkflowId());
        } catch (Exception e) {
            lockActivities.logLockEvent(lockHandle.getLockKey(), "LOCK_RELEASE_FAILED", e.getMessage());
        }
    }

    @Override
    public String getLockStatus() {
        return lockStatus;
    }

    private String buildLockKey(FileMetadata fileMetadata) {
        return "file_lock:" + fileMetadata.getFilePath() + ":" + fileMetadata.getFileChecksum();
    }
}
