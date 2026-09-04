package com.example.temporalshowcase.workflows.pattern6;

import com.example.temporalshowcase.models.FileFormat;
import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.StandardizedData;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface FixedWidthProcessorWorkflow {
    @WorkflowMethod(name = "P06-FixedWidthProcessor")
    StandardizedData processFixedWidth(FileMetadata fileMetadata, FileFormat format);
}
