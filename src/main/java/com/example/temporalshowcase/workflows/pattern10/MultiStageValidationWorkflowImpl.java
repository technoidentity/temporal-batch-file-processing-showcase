package com.example.temporalshowcase.workflows.pattern10;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.ValidationActivities;
import com.example.temporalshowcase.models.*;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Workflow;


/**
 * Pattern 10: Multi-Stage Validation implementation.
 *
 * Four sequential validation gates:
 *   BASIC → BUSINESS → ADVANCED → FINAL
 * Any failure triggers a stage-appropriate remediation.  After remediation
 * validation restarts from BASIC.  If MAX_REMEDIATION_ATTEMPTS is reached
 * the file is escalated for manual data-team review.
 */
public class MultiStageValidationWorkflowImpl implements MultiStageValidationWorkflow {

    private volatile String currentStage = "INITIALIZING";

    private final ValidationActivities validationActivities = Workflow.newActivityStub(
            ValidationActivities.class,
            TemporalRuntime.activity("validation"));

    @Override
    public ValidationResult executeMultiStageValidation(FileMetadata fileMetadata) {
        ValidationContext context = new ValidationContext(fileMetadata);

        while (!context.isComplete()) {
            currentStage = context.getCurrentStage().name();

            switch (context.getCurrentStage()) {
                case BASIC_VALIDATION -> runBasicValidation(context);
                case BUSINESS_VALIDATION -> runBusinessValidation(context);
                case ADVANCED_VALIDATION -> runAdvancedValidation(context);
                case FINAL_VALIDATION -> runFinalValidation(context);
                case REMEDIATION -> runRemediation(context);
                case MANUAL_REVIEW -> {
                    validationActivities.escalateForManualDataReview(
                            context.getFileMetadata(), context.getValidationErrors());
                    throw ApplicationFailure.newFailure(
                            "Data quality validation failed after " + context.getRemediationAttempts()
                                    + " remediation attempts; escalated for manual review",
                            "VALIDATION_ESCALATED");
                }
                default -> context.markComplete();
            }
        }

        currentStage = "COMPLETE";
        return context.getFinalResult();
    }

    @Override
    public String getCurrentValidationStage() {
        return currentStage;
    }

    private void runBasicValidation(ValidationContext context) {
        ValidationResult result = validationActivities.executeBasicValidation(context.getFileMetadata());
        context.setBasicResult(result);
        if (result.isValid()) {
            context.advanceToStage(ValidationStage.BUSINESS_VALIDATION);
        } else {
            context.setValidationErrors(result.getErrors());
            context.advanceToStage(ValidationStage.REMEDIATION);
        }
    }

    private void runBusinessValidation(ValidationContext context) {
        ValidationResult result = validationActivities.executeBusinessValidation(
                context.getFileMetadata(), context.getBasicResult());
        context.setBusinessResult(result);
        if (result.isValid()) {
            context.advanceToStage(ValidationStage.ADVANCED_VALIDATION);
        } else {
            context.setValidationErrors(result.getErrors());
            context.advanceToStage(ValidationStage.REMEDIATION);
        }
    }

    private void runAdvancedValidation(ValidationContext context) {
        ValidationResult result = validationActivities.executeAdvancedValidation(
                context.getFileMetadata(), context.getBusinessResult());
        context.setAdvancedResult(result);
        if (result.isValid()) {
            context.advanceToStage(ValidationStage.FINAL_VALIDATION);
        } else {
            context.setValidationErrors(result.getErrors());
            context.advanceToStage(ValidationStage.REMEDIATION);
        }
    }

    private void runFinalValidation(ValidationContext context) {
        ValidationResult result = validationActivities.executeFinalValidation(
                context.getFileMetadata(), context.getAllValidationResults());
        if (result.isValid()) {
            context.setFinalResult(result);
            context.markComplete();
        } else {
            context.setValidationErrors(result.getErrors());
            context.advanceToStage(ValidationStage.REMEDIATION);
        }
    }

    /**
     * Stage-aware repair dispatcher. Selects the appropriate repair activities based on which
     * validation stage failed, then either resets back to BASIC_VALIDATION or advances to
     * MANUAL_REVIEW if repair fails or MAX_REMEDIATION_ATTEMPTS is reached.
     */
    private void runRemediation(ValidationContext context) {
        if (context.hasExceededRemediationAttempts(
                TemporalRuntime.patterns().getValidation().getMaxRemediationAttempts())) {
            context.advanceToStage(ValidationStage.MANUAL_REVIEW);
            return;
        }

        ValidationStage failedAt = context.getFailedStage();

        if (failedAt == ValidationStage.BASIC_VALIDATION) {
            boolean formatFixed = validationActivities.applyFormatCorrection(
                    context.getFileMetadata(), context.getValidationErrors());
            boolean dataFixed = validationActivities.applyDataCleansing(
                    context.getFileMetadata(), context.getValidationErrors());
            boolean basicFixed = formatFixed || dataFixed ||
                    validationActivities.attemptBasicRemediation(
                            context.getFileMetadata(), context.getValidationErrors());

            if (basicFixed) {
                context.clearValidationErrors();
                context.resetToStage(ValidationStage.BASIC_VALIDATION);
            } else {
                context.advanceToStage(ValidationStage.MANUAL_REVIEW);
            }

        } else if (failedAt == ValidationStage.BUSINESS_VALIDATION) {
            boolean missingFixed = validationActivities.correctMissingData(
                    context.getFileMetadata(), context.getValidationErrors());
            boolean businessFixed = missingFixed ||
                    validationActivities.attemptBusinessRemediation(
                            context.getFileMetadata(), context.getValidationErrors());

            if (businessFixed) {
                validationActivities.approveProcessingAndMarkAsQuality(
                        context.getFileMetadata(), "Business remediation succeeded");
                context.clearValidationErrors();
                context.markApprovedAfterRemediation();
            } else {
                context.advanceToStage(ValidationStage.MANUAL_REVIEW);
            }

        } else {
            // Advanced/Final failure: apply outlier detection and ML-based fixes
            boolean outliersFixed = validationActivities.detectAndCorrectOutliers(
                    context.getFileMetadata(), context.getValidationErrors());
            boolean advancedFixed = outliersFixed ||
                    validationActivities.attemptAdvancedRemediation(
                            context.getFileMetadata(), context.getValidationErrors());

            if (advancedFixed) {
                context.clearValidationErrors();
                context.resetToStage(ValidationStage.BASIC_VALIDATION);
            } else {
                context.advanceToStage(ValidationStage.MANUAL_REVIEW);
            }
        }
    }
}
