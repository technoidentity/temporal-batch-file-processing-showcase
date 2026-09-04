package com.example.temporalshowcase.workflows.pattern6;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.FormatDetectionActivities;
import com.example.temporalshowcase.models.FileFormat;
import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.StandardizedData;
import io.temporal.workflow.Workflow;


public class CsvProcessorWorkflowImpl implements CsvProcessorWorkflow {

    private final FormatDetectionActivities activities = Workflow.newActivityStub(
            FormatDetectionActivities.class,
            TemporalRuntime.activity("format-parse"));

    @Override
    public StandardizedData processCsv(FileMetadata fileMetadata, FileFormat format) {
        return activities.parseCsvFile(fileMetadata, format);
    }
}
