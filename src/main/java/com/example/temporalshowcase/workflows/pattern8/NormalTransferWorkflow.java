package com.example.temporalshowcase.workflows.pattern8;

import com.example.temporalshowcase.models.TransferRequest;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 8 – Network OK branch: direct transfer without resumable checkpoint logic.
 */
@WorkflowInterface
public interface NormalTransferWorkflow {

    @WorkflowMethod(name = "P08-NormalTransfer")
    void executeNormalTransfer(TransferRequest transferRequest);
}
