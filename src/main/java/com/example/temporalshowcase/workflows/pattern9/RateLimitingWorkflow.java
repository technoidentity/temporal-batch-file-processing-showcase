package com.example.temporalshowcase.workflows.pattern9;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 9: Resource Exhaustion — Rate Limiting
 *
 * Solves: System resource exhaustion from high file processing volumes.
 * Temporal features: Long-running monitoring workflow with Workflow.sleep(),
 *                    parallel activity fan-out for resource metrics,
 *                    adaptive throttle rate calculation, Promise.allOf().
 */
@WorkflowInterface
public interface RateLimitingWorkflow {

    /** Starts a continuous monitoring loop that self-terminates after maxCycles iterations. */
    @WorkflowMethod(name = "P09-AdaptiveRateLimiting")
    void manageRateLimiting(int maxCycles);

    /** Returns the current throttle status — e.g. "NORMAL load=0.42" or "WAITING_FOR_RESOURCES throttle=HEAVY". */
    @QueryMethod
    String getCurrentThrottleStatus();
}
