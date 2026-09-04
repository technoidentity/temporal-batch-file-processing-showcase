package com.example.temporalshowcase.workflows.pattern11;

import com.example.temporalshowcase.models.LockHandle;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 11 – Lock Denied path: polls for lock availability with backoff strategy.
 *
 * Steps:
 *  1. Check Lock Status / Poll for availability
 *  2. Wait for Lock / Backoff strategy
 *     ├─ Lock Available → returns the acquired LockHandle
 *     └─ Still Locked  → 3. Timeout / Escalate
 */
@WorkflowInterface
public interface LockWaitWorkflow {

    @WorkflowMethod(name = "P11-LockWait")
    LockHandle waitForLock(String lockKey, String requesterWorkflowId, int priority);

    /** Returns current wait state — e.g. "POLLING attempt=3" or "LOCK_ACQUIRED" or "TIMED_OUT". */
    @QueryMethod
    String getLockWaitStatus();
}
