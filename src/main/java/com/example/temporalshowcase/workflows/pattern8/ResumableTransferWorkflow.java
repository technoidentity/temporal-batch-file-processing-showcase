package com.example.temporalshowcase.workflows.pattern8;

import com.example.temporalshowcase.models.TransferRequest;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 8: Network Failures — Resumable Transfer
 *
 * Solves: Network connectivity issues causing file transfer interruptions.
 * Temporal features: Checkpoint-based state, Workflow.sleep() for recovery wait,
 *                    retry with exponential backoff, durable progress tracking.
 */
@WorkflowInterface
public interface ResumableTransferWorkflow {

    @WorkflowMethod(name = "P08-ResumableTransfer")
    void executeResumableTransfer(TransferRequest transferRequest);

    /** Returns live transfer progress — e.g. "TRANSFERRING — 75/200 KB (37%)" */
    @QueryMethod
    String getTransferProgress();
}
