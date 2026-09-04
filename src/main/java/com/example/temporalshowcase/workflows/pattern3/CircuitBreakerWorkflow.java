package com.example.temporalshowcase.workflows.pattern3;

import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.ProcessingResult;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 3: Empty Files — Circuit Breaker
 *
 * Solves: Empty files wasting resources and tripping downstream systems.
 * Temporal features: Workflow state machine, Workflow.currentTimeMillis(),
 *                    sliding-window failure rate, CLOSED/OPEN/HALF_OPEN states.
 */
@WorkflowInterface
public interface CircuitBreakerWorkflow {

    @WorkflowMethod(name = "P03-EmptyFileCircuitBreaker")
    ProcessingResult processFileWithCircuitBreaker(FileMetadata fileMetadata);

    /** Returns the current circuit breaker state (CLOSED / OPEN / HALF_OPEN). */
    @QueryMethod
    String getCircuitState();
}
