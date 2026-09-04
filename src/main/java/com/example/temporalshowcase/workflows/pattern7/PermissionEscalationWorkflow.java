package com.example.temporalshowcase.workflows.pattern7;

import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.PermissionIssue;
import com.example.temporalshowcase.models.ProcessingResult;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 7: Permission Issues — Escalation Pattern
 *
 * Solves: File access permission problems causing processing failures.
 * Temporal features: @SignalMethod for human-in-the-loop, Workflow.await(),
 *                    multi-level auto-fix strategies, @QueryMethod for live status.
 */
@WorkflowInterface
public interface PermissionEscalationWorkflow {

    @WorkflowMethod(name = "P07-PermissionEscalation")
    ProcessingResult resolvePermissionIssue(FileMetadata fileMetadata, PermissionIssue permissionIssue);

    /** Human operator signals after manually resolving the permission issue. */
    @SignalMethod
    void permissionResolved(String resolution);

    /**
     * Update variant of the human resolution: unlike the fire-and-forget signal,
     * an {@code @UpdateMethod} is synchronous and returns an acknowledgement, so
     * an operator UI gets confirmation the resolution was accepted and applied.
     */
    @UpdateMethod
    String resolvePermission(String resolution);

    /** Returns the current escalation step (e.g. CHECKING, AUTO_FIX, WAITING_HUMAN, RESOLVED). */
    @QueryMethod
    String getEscalationStatus();
}
