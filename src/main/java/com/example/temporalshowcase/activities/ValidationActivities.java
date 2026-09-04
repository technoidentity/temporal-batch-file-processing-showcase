package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.util.List;

@ActivityInterface
public interface ValidationActivities {

    /** Stage 1: checks file existence, readability, and non-zero size. */
    @ActivityMethod
    ValidationResult executeBasicValidation(FileMetadata fileMetadata);

    /** Stage 2: checks content structure (CSV header columns, XML well-formedness, JSON shape). */
    @ActivityMethod
    ValidationResult executeBusinessValidation(FileMetadata fileMetadata, ValidationResult basicResult);

    /** Stage 3: scans content for corruption markers, invalid dates, and unknown codes. */
    @ActivityMethod
    ValidationResult executeAdvancedValidation(FileMetadata fileMetadata, ValidationResult businessResult);

    /** Stage 4: aggregates errors and warnings from all earlier stages into a final verdict. */
    @ActivityMethod
    ValidationResult executeFinalValidation(FileMetadata fileMetadata, List<ValidationResult> allResults);

    @ActivityMethod
    boolean attemptBasicRemediation(FileMetadata fileMetadata, List<String> errors);

    @ActivityMethod
    boolean attemptBusinessRemediation(FileMetadata fileMetadata, List<String> errors);

    @ActivityMethod
    boolean attemptAdvancedRemediation(FileMetadata fileMetadata, List<String> errors);

    @ActivityMethod
    void escalateForManualDataReview(FileMetadata fileMetadata, List<String> errors);

    // ── Repair stages ──────────────────────────────────────────────────────

    /** Format Correction / Auto-fix — fixes structural issues (delimiters, encoding, line endings). */
    @ActivityMethod
    boolean applyFormatCorrection(FileMetadata fileMetadata, List<String> errors);

    /** Data Cleansing / Standardization — normalizes dates, trims whitespace, standardizes codes. */
    @ActivityMethod
    boolean applyDataCleansing(FileMetadata fileMetadata, List<String> errors);

    /** Missing Data Correction — fills default values for nullable fields. */
    @ActivityMethod
    boolean correctMissingData(FileMetadata fileMetadata, List<String> errors);

    /** Outlier Detection Correction — flags and corrects statistical outliers. */
    @ActivityMethod
    boolean detectAndCorrectOutliers(FileMetadata fileMetadata, List<String> errors);

    /** Approve Processing / Mark as quality — used after successful business remediation. */
    @ActivityMethod
    void approveProcessingAndMarkAsQuality(FileMetadata fileMetadata, String reason);
}
