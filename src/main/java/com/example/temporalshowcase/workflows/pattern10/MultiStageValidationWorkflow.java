package com.example.temporalshowcase.workflows.pattern10;

import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.ValidationResult;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 10: Data Quality — Multi-Stage Validation
 *
 * Solves: Data quality problems causing downstream processing failures.
 * Temporal features: State-machine workflow, iterative remediation,
 *                    @QueryMethod for validation progress, configurable
 *                    max remediation attempts.
 */
@WorkflowInterface
public interface MultiStageValidationWorkflow {

    @WorkflowMethod(name = "P10-DataQualityValidation")
    ValidationResult executeMultiStageValidation(FileMetadata fileMetadata);

    /** Returns the name of the validation stage currently executing. */
    @QueryMethod
    String getCurrentValidationStage();
}
