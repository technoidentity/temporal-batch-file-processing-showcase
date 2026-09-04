package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface IdempotencyActivities {

    /** Returns the persisted processing state for the given hash, or {@code null} if not yet seen. */
    @ActivityMethod
    ProcessingState checkProcessingState(String fileHash);

    @ActivityMethod
    void recordProcessingStart(ProcessingState state);

    /**
     * Atomically claims the idempotency key: records the IN_PROGRESS state only
     * if no state exists yet. Returns {@code true} if this caller won the claim,
     * {@code false} if a concurrent duplicate already started. This closes the
     * check-then-act race that {@code checkProcessingState} + {@code recordProcessingStart}
     * leaves open.
     */
    @ActivityMethod
    boolean tryBeginProcessing(ProcessingState state);

    @ActivityMethod
    void updateProcessingState(ProcessingState state);

    @ActivityMethod
    void logDuplicateDetected(String fileHash, ProcessingState existingState);

    /** Audit Trail Log — records every state transition for compliance and traceability. */
    @ActivityMethod
    void logAuditTrail(String fileHash, String event, String details);
}
