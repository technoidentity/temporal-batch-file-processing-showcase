package com.example.temporalshowcase.workflows.pattern11;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.LockActivities;
import com.example.temporalshowcase.models.*;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Workflow;

import java.time.Duration;

/**
 * Pattern 11 – Lock Wait Workflow (Lock Denied path).
 *
 * 3-step flow:
 *  1. Check Lock Status — poll for availability (with owner liveness check)
 *  2. Wait for Lock    — exponential backoff strategy between polls
 *     ├─ Lock Available → acquire and return LockHandle
 *     └─ Still Locked  → 3. Timeout / Escalate
 */
public class LockWaitWorkflowImpl implements LockWaitWorkflow {

    private final int      MAX_WAIT_ATTEMPTS = TemporalRuntime.patterns().getLocking().getWaitMaxAttempts();
    private final Duration MAX_WAIT_TIMEOUT  = TemporalRuntime.patterns().getLocking().getWaitMaxTimeout();
    private final Duration BASE_BACKOFF      = TemporalRuntime.patterns().getLocking().getWaitBaseBackoff();

    private final LockActivities lockActivities = Workflow.newActivityStub(
            LockActivities.class,
            TemporalRuntime.activity("lock-wait"));

    private String lockWaitStatus = "STARTING";

    @Override
    public LockHandle waitForLock(String lockKey, String requesterWorkflowId, int priority) {
        long startTime = Workflow.currentTimeMillis();

        for (int attempt = 1; attempt <= MAX_WAIT_ATTEMPTS; attempt++) {

            // ── Step 1: Check Lock Status / Poll for availability ──
            lockWaitStatus = "POLLING attempt=" + attempt + "/" + MAX_WAIT_ATTEMPTS;
            LockHandle currentHolder = lockActivities.getLockInfo(lockKey);

            // Check if current holder is a dead/expired workflow — if so, force-release
            if (currentHolder != null) {
                boolean holderActive = lockActivities.isWorkflowActive(currentHolder.getOwnerWorkflowId());
                boolean lockExpired  = currentHolder.getExpiresAt() < Workflow.currentTimeMillis();

                if (!holderActive || lockExpired) {
                    lockWaitStatus = "FORCE_RELEASING_EXPIRED_LOCK";
                    lockActivities.forceReleaseLock(lockKey);
                    lockActivities.logLockEvent(lockKey, "FORCE_RELEASED_BY_WAIT_WORKFLOW", currentHolder.getOwnerWorkflowId());
                }
            }

            // Try to acquire with priority-based and FCFS conflict resolution
            LockResult result = lockActivities.acquireLock(
                    LockRequest.builder()
                            .lockKey(lockKey)
                            .lockTimeout(Duration.ofMinutes(30))
                            .requesterWorkflowId(requesterWorkflowId)
                            .priority(priority)
                            .build());

            if (result.isSuccess()) {
                lockWaitStatus = "LOCK_ACQUIRED after " + attempt + " attempt(s)";
                lockActivities.logLockEvent(lockKey, "LOCK_ACQUIRED_AFTER_WAIT", requesterWorkflowId);

                // Conflict resolution: apply FCFS and priority policies
                lockActivities.resolveByPriority(lockKey, priority);
                lockActivities.resolveByFCFS(lockKey, requesterWorkflowId);
                return result.getLockHandle();
            }

            if (Workflow.currentTimeMillis() - startTime >= MAX_WAIT_TIMEOUT.toMillis()) {
                break;
            }

            // ── Step 2: Wait for Lock / Backoff strategy ──
            long backoffSec = Math.min(BASE_BACKOFF.toSeconds() * (long) Math.pow(2, attempt - 1), 60);
            lockWaitStatus = "WAITING backoff=" + backoffSec + "s attempt=" + attempt;
            lockActivities.logLockEvent(lockKey, "WAITING_BACKOFF_" + backoffSec + "s", requesterWorkflowId);
            Workflow.sleep(Duration.ofSeconds(backoffSec));
        }

        // ── Step 3: Still Locked → Timeout / Escalate ──
        lockWaitStatus = "TIMED_OUT — lock never became available";
        lockActivities.timeoutBasedRelease(lockKey);
        lockActivities.logLockEvent(lockKey, "LOCK_WAIT_TIMEOUT_ESCALATED", requesterWorkflowId);
        throw ApplicationFailure.newFailure(
                "Lock wait timed out after " + MAX_WAIT_ATTEMPTS + " attempts for key: " + lockKey,
                "LOCK_WAIT_TIMEOUT");
    }

    @Override
    public String getLockWaitStatus() {
        return lockWaitStatus;
    }
}
