package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.util.List;

@ActivityInterface
public interface FormatDetectionActivities {

    /**
     * Runs the 4-step detection pipeline: magic bytes → content inspection →
     * schema validation → extension fallback. Returns the resolved format and confidence level.
     */
    @ActivityMethod
    FileFormat analyzeAndDetectFormat(FileMetadata fileMetadata);

    @ActivityMethod
    StandardizedData parseCsvFile(FileMetadata fileMetadata, FileFormat format);

    @ActivityMethod
    StandardizedData parseXmlFile(FileMetadata fileMetadata, FileFormat format);

    @ActivityMethod
    StandardizedData parseJsonFile(FileMetadata fileMetadata, FileFormat format);

    @ActivityMethod
    StandardizedData parseFixedWidthFile(FileMetadata fileMetadata, FileFormat format);

    @ActivityMethod
    StandardizedData attemptFormatConversion(FileMetadata fileMetadata, FileFormat format);

    @ActivityMethod
    void escalateUnknownFormat(FileMetadata fileMetadata, FileFormat format, List<String> errors);

    /** Error Handling — Format Error Handler */
    @ActivityMethod
    void handleFormatError(FileMetadata fileMetadata, FileFormat format, String errorMessage);

    /** Error Handling — Conversion Error Handler */
    @ActivityMethod
    void handleConversionError(FileMetadata fileMetadata, String errorMessage);

    /** Error Handling — Validation Error Handler */
    @ActivityMethod
    void handleValidationError(FileMetadata fileMetadata, FileFormat format, String errorMessage);
}
