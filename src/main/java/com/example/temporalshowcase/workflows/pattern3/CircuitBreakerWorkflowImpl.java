package com.example.temporalshowcase.workflows.pattern3;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.CircuitBreakerActivities;
import com.example.temporalshowcase.activities.FileManagementActivities;
import com.example.temporalshowcase.models.*;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Workflow;

import java.time.Duration;

/**
 * Pattern 3: Circuit Breaker for empty-file storms.
 *
 * Flow for empty files:
 *   1. Log + update sliding-window metrics (last 10 files)
 *   2. Check failure rate:
 *        Rate > 50%  → OPEN  circuit (stop ALL processing for RECOVERY_TIMEOUT)
 *        Rate <= 50% → HALF_OPEN (allow one probe, then decide to CLOSE or re-OPEN)
 *
 * Flow for files with content:
 *   - If OPEN   → throw CIRCUIT_BREAKER_OPEN (after 5 min → try HALF_OPEN)
 *   - If HALF_OPEN → process as probe: success → CLOSE, failure → re-OPEN
 *   - If CLOSED → normal processing
 */
public class CircuitBreakerWorkflowImpl implements CircuitBreakerWorkflow {

    private static final String   CIRCUIT_KEY      = "file_processing_circuit_breaker";
    private final Duration        RECOVERY_TIMEOUT = TemporalRuntime.patterns().getCircuitBreaker().getRecoveryTimeout();

    private final FileManagementActivities fileActivities = Workflow.newActivityStub(
            FileManagementActivities.class,
            TemporalRuntime.activity("file-ops"));

    private final CircuitBreakerActivities circuitActivities = Workflow.newActivityStub(
            CircuitBreakerActivities.class,
            TemporalRuntime.activity("quick"));

    // Tracked for @QueryMethod
    private String currentState = "CLOSED";

    // ──────────────────────────────────────────────────────────────────────────

    @Override
    public ProcessingResult processFileWithCircuitBreaker(FileMetadata fileMetadata) {

        CircuitBreakerState circuitState = circuitActivities.getCircuitBreakerState(CIRCUIT_KEY);
        currentState = circuitState.getState().name();

        FileValidationResult validation = fileActivities.validateFile(fileMetadata);

        if (validation.isEmpty()) {
            // ── "File is empty" path ──────────────────────────────────────────
            return handleEmptyFile(circuitState, fileMetadata);
        }

        // ── "File has content" path ───────────────────────────────────────────
        // Check if OPEN circuit should block or transition to HALF_OPEN
        if (circuitState.isOpen()) {
            long elapsed = Workflow.currentTimeMillis() - circuitState.getLastFailureTime();
            if (elapsed > RECOVERY_TIMEOUT.toMillis()) {
                // Wait 5 minutes passed → transition to HALF_OPEN for sample processing
                circuitActivities.updateCircuitBreakerState(
                        new CircuitBreakerUpdate(CIRCUIT_KEY, CircuitState.HALF_OPEN));
                circuitState.setState(CircuitState.HALF_OPEN);
                currentState = "HALF_OPEN";
                circuitActivities.recordCircuitBreakerEvent(CIRCUIT_KEY,
                        "HALF_OPEN", "Recovery timeout elapsed, allowing probe");
            } else {
                // Still within recovery window — stop processing
                currentState = "OPEN";
                throw ApplicationFailure.newFailure(
                        "Circuit breaker OPEN — processing blocked for " + RECOVERY_TIMEOUT.toMinutes() + " min",
                        "CIRCUIT_BREAKER_OPEN");
            }
        }

        // Step 3: Process the file (CLOSED or HALF_OPEN probe)
        try {
            ProcessingResult result = fileActivities.processFileContent(fileMetadata);
            circuitActivities.recordSuccessfulFile(CIRCUIT_KEY);

            if (circuitState.isHalfOpen()) {
                // Probe succeeded → Close Circuit, Normal Processing
                circuitActivities.updateCircuitBreakerState(
                        new CircuitBreakerUpdate(CIRCUIT_KEY, CircuitState.CLOSED));
                circuitActivities.recordCircuitBreakerEvent(CIRCUIT_KEY,
                        "CLOSED", "Half-open probe succeeded — circuit closed");
                currentState = "CLOSED";
            }
            return result;

        } catch (Exception e) {
            // Probe failed → re-Open Circuit, Stop Processing
            circuitActivities.updateCircuitBreakerState(
                    new CircuitBreakerUpdate(CIRCUIT_KEY, CircuitState.OPEN));
            circuitActivities.recordCircuitBreakerEvent(CIRCUIT_KEY,
                    "OPENED", "Processing failure: " + e.getMessage());
            currentState = "OPEN";
            throw e;
        }
    }

    /**
     * Handles an empty file: log it, update metrics, then open or half-open the circuit.
     *
     * Log → Update Metrics → Check Failure Rate (last 10 files):
     *   Rate > 50%  → OPEN  circuit
     *   Rate <= 50% → HALF_OPEN (sample the next non-empty file before deciding)
     */
    private ProcessingResult handleEmptyFile(CircuitBreakerState circuitState, FileMetadata fileMetadata) {
        fileActivities.logEmptyFile(fileMetadata);

        // Update Metrics (maintains sliding window of last 10 files)
        circuitActivities.updateEmptyFileMetrics(CIRCUIT_KEY);

        // Reload updated state so we get the fresh window counts
        CircuitBreakerState updated = circuitActivities.getCircuitBreakerState(CIRCUIT_KEY);

        double rate = updated.getFailureRate();
        circuitActivities.recordCircuitBreakerEvent(CIRCUIT_KEY, "EMPTY_FILE",
                String.format("window=[%d/%d] rate=%.0f%%",
                        updated.getWindowEmpty(), updated.getWindowTotal(), rate * 100));

        if (updated.isHighFailureRate(
                TemporalRuntime.patterns().getCircuitBreaker().getMinSamples(),
                TemporalRuntime.patterns().getCircuitBreaker().getFailureRateThreshold())) {
            // Rate > 50% → Open Circuit, Stop Processing
            circuitActivities.updateCircuitBreakerState(
                    new CircuitBreakerUpdate(CIRCUIT_KEY, CircuitState.OPEN));
            circuitActivities.recordCircuitBreakerEvent(CIRCUIT_KEY, "OPENED",
                    "Empty file rate " + String.format("%.0f%%", rate * 100) + " > 50% threshold");
            currentState = "OPEN";
        } else if (!circuitState.isOpen()) {
            // Rate <= 50% → Half Open, Sample Processing
            circuitActivities.updateCircuitBreakerState(
                    new CircuitBreakerUpdate(CIRCUIT_KEY, CircuitState.HALF_OPEN));
            circuitActivities.recordCircuitBreakerEvent(CIRCUIT_KEY, "HALF_OPEN",
                    "Empty file rate " + String.format("%.0f%%", rate * 100) + " <= 50%, sampling next file");
            currentState = "HALF_OPEN";
        }

        return ProcessingResult.builder()
                .status(ProcessingStatus.SKIPPED)
                .message(String.format("Empty file skipped — circuit=%s rate=%.0f%%", currentState, rate * 100))
                .build();
    }

    @Override
    public String getCircuitState() {
        return currentState;
    }
}
