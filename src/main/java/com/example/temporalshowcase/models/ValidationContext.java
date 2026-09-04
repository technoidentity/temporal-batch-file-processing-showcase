package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ValidationContext {
    private FileMetadata fileMetadata;
    private ValidationStage currentStage;
    private ValidationStage failedStage;
    private List<String> validationErrors;
    private ValidationResult basicResult;
    private ValidationResult businessResult;
    private ValidationResult advancedResult;
    private ValidationResult finalResult;
    private boolean complete;
    private int remediationAttempts;

    public ValidationContext(FileMetadata fileMetadata) {
        this.fileMetadata = fileMetadata;
        this.currentStage = ValidationStage.BASIC_VALIDATION;
        this.validationErrors = new ArrayList<>();
        this.complete = false;
        this.remediationAttempts = 0;
    }

    public void advanceToStage(ValidationStage stage) {
        this.currentStage = stage;
        if (stage == ValidationStage.COMPLETE) {
            this.complete = true;
        }
    }

    public void setValidationErrors(List<String> errors) {
        this.failedStage = this.currentStage;
        this.validationErrors = errors;
    }

    public void clearValidationErrors() {
        this.validationErrors = new ArrayList<>();
        this.failedStage = null;
    }

    public void resetToStage(ValidationStage stage) {
        this.currentStage = stage;
        this.remediationAttempts++;
    }

    public void markComplete() {
        this.complete = true;
        this.currentStage = ValidationStage.COMPLETE;
    }

    public boolean hasExceededRemediationAttempts(int maxAttempts) {
        return remediationAttempts >= maxAttempts;
    }

    /** Business Remediation success path — marks file as approved without re-running all stages. */
    public void markApprovedAfterRemediation() {
        this.finalResult = ValidationResult.passed(ValidationStage.FINAL_VALIDATION);
        this.finalResult.setMessage("Approved after business remediation");
        this.complete = true;
        this.currentStage = ValidationStage.COMPLETE;
    }

    public List<ValidationResult> getAllValidationResults() {
        List<ValidationResult> results = new ArrayList<>();
        if (basicResult != null) results.add(basicResult);
        if (businessResult != null) results.add(businessResult);
        if (advancedResult != null) results.add(advancedResult);
        return results;
    }
}
