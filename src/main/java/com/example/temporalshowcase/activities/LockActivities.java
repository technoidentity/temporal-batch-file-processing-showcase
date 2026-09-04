package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface LockActivities {

    /** Atomically acquires the lock if not held (or if the existing lock has expired). */
    @ActivityMethod
    LockResult acquireLock(LockRequest lockRequest);

    @ActivityMethod
    void releaseLock(LockHandle lockHandle);

    @ActivityMethod
    void forceReleaseLock(String lockKey);

    /** Returns the current lock holder, or {@code null} if the lock is free. */
    @ActivityMethod
    LockHandle getLockInfo(String lockKey);

    /** Checks via Temporal's visibility API whether the holder workflow is still running. */
    @ActivityMethod
    boolean isWorkflowActive(String workflowId);

    @ActivityMethod
    void logLockEvent(String lockKey, String event, String workflowId);

    // ── Conflict Resolution (bottom left box) ──────────────────────────────

    /** Priority-Based Resolution — higher priority waiters get the lock first. */
    @ActivityMethod
    void resolveByPriority(String lockKey, int requesterPriority);

    /** First-Come-First-Serve Policy — tracks waiter queue by arrival time. */
    @ActivityMethod
    void resolveByFCFS(String lockKey, String requesterWorkflowId);

    /** Timeout-Based Release — forcibly releases a lock that has exceeded its timeout. */
    @ActivityMethod
    void timeoutBasedRelease(String lockKey);

    /** Manual Intervention Override — allows operator to force-release any lock. */
    @ActivityMethod
    void manualInterventionOverride(String lockKey, String reason);

    // ── Lock Management (left box) ─────────────────────────────────────────

    /** Notify waiting processes that the lock has been released. */
    @ActivityMethod
    void notifyWaitingProcesses(String lockKey);
}
