package com.example.temporalshowcase.workflows.pattern6;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.FormatDetectionActivities;
import com.example.temporalshowcase.models.*;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.*;

import java.time.Duration;
import java.util.List;

/**
 * Pattern 6: Multi-Format Handler.
 *
 * Format Detection (4 steps inside analyzeAndDetectFormat activity):
 *   1. File Header Analysis — magic bytes
 *   2. Content Inspection   — pattern matching
 *   3. Schema Validation    — format-specific rules
 *   4. Format Classification — determine processor
 *
 * Routing:
 *   CSV / XML / JSON / Fixed-Width → dedicated child workflow → StandardizedData
 *   Unknown → attempt conversion → success: StandardizedData | failure: escalation
 *
 * Error Handling:
 *   Format errors, Conversion errors, Validation errors → dedicated handler activities
 */
public class MultiFormatHandlerWorkflowImpl implements MultiFormatHandlerWorkflow {

    private final FormatDetectionActivities formatActivities = Workflow.newActivityStub(
            FormatDetectionActivities.class,
            TemporalRuntime.activity("format-detect"));

    // Tracked for @QueryMethod
    private String currentFormat = "DETECTING";

    // ──────────────────────────────────────────────────────────────────────────

    @Override
    public StandardizedData handleMultiFormatFile(FileMetadata fileMetadata) {
        // Steps 1-4: Format Detection Service (magic bytes → content → schema → classification)
        FileFormat format;
        try {
            format = formatActivities.analyzeAndDetectFormat(fileMetadata);
        } catch (Exception e) {
            formatActivities.handleFormatError(fileMetadata,
                    FileFormat.builder().type(FileFormatType.UNKNOWN).detectionConfidence("LOW").build(),
                    e.getMessage());
            throw ApplicationFailure.newFailure("Format detection failed: " + e.getMessage(), "FORMAT_ERROR");
        }
        currentFormat = format.getType().name();

        if ("LOW".equals(format.getDetectionConfidence())) {
            formatActivities.handleValidationError(fileMetadata, format,
                    "Low-confidence format detection — proceeding with caution");
        }

        try {
            return switch (format.getType()) {
                case CSV         -> processCsvFile(fileMetadata, format);
                case XML         -> processXmlFile(fileMetadata, format);
                case JSON        -> processJsonFile(fileMetadata, format);
                case FIXED_WIDTH -> processFixedWidthFile(fileMetadata, format);
                case UNKNOWN     -> attemptConversionOrEscalate(fileMetadata, format);
            };
        } catch (ApplicationFailure af) {
            throw af; // re-throw workflow failures as-is
        } catch (Exception e) {
            formatActivities.handleFormatError(fileMetadata, format, e.getMessage());
            throw ApplicationFailure.newFailure("Format processing failed: " + e.getMessage(), "FORMAT_ERROR");
        }
    }

    private StandardizedData processCsvFile(FileMetadata fileMetadata, FileFormat format) {
        CsvProcessorWorkflow child = Workflow.newChildWorkflowStub(
                CsvProcessorWorkflow.class,
                ChildWorkflowOptions.newBuilder()
                        .setTaskQueue(TemporalRuntime.queue("multi-format"))
                        .setWorkflowId("p06-csv-" + Workflow.getInfo().getWorkflowId())
                        .setWorkflowExecutionTimeout(Duration.ofHours(1)).build());
        return child.processCsv(fileMetadata, format);
    }

    private StandardizedData processXmlFile(FileMetadata fileMetadata, FileFormat format) {
        XmlProcessorWorkflow child = Workflow.newChildWorkflowStub(
                XmlProcessorWorkflow.class,
                ChildWorkflowOptions.newBuilder()
                        .setTaskQueue(TemporalRuntime.queue("multi-format"))
                        .setWorkflowId("p06-xml-" + Workflow.getInfo().getWorkflowId())
                        .setWorkflowExecutionTimeout(Duration.ofHours(1)).build());
        return child.processXml(fileMetadata, format);
    }

    private StandardizedData processJsonFile(FileMetadata fileMetadata, FileFormat format) {
        JsonProcessorWorkflow child = Workflow.newChildWorkflowStub(
                JsonProcessorWorkflow.class,
                ChildWorkflowOptions.newBuilder()
                        .setTaskQueue(TemporalRuntime.queue("multi-format"))
                        .setWorkflowId("p06-json-" + Workflow.getInfo().getWorkflowId())
                        .setWorkflowExecutionTimeout(Duration.ofHours(1)).build());
        return child.processJson(fileMetadata, format);
    }

    private StandardizedData processFixedWidthFile(FileMetadata fileMetadata, FileFormat format) {
        FixedWidthProcessorWorkflow child = Workflow.newChildWorkflowStub(
                FixedWidthProcessorWorkflow.class,
                ChildWorkflowOptions.newBuilder()
                        .setTaskQueue(TemporalRuntime.queue("multi-format"))
                        .setWorkflowId("p06-fixed-" + Workflow.getInfo().getWorkflowId())
                        .setWorkflowExecutionTimeout(Duration.ofHours(1)).build());
        return child.processFixedWidth(fileMetadata, format);
    }

    private StandardizedData attemptConversionOrEscalate(FileMetadata fileMetadata, FileFormat format) {
        try {
            StandardizedData converted = formatActivities.attemptFormatConversion(fileMetadata, format);
            if (converted != null && converted.getRecordCount() > 0) {
                return converted;
            }
        } catch (Exception e) {
            formatActivities.handleConversionError(fileMetadata, e.getMessage());
        }
        formatActivities.escalateUnknownFormat(fileMetadata, format,
                List.of("Format detection failed", "Conversion failed — manual review required"));
        throw ApplicationFailure.newFailure(
                "Unknown file format — escalated for manual review: " + fileMetadata.getFilePath(),
                "UNKNOWN_FORMAT");
    }

    @Override
    public String getCurrentFormat() {
        return currentFormat;
    }
}
