package com.example.temporalshowcase.workflows.pattern6;

import com.example.temporalshowcase.models.FileFormat;
import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.StandardizedData;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface JsonProcessorWorkflow {
    @WorkflowMethod(name = "P06-JsonProcessor")
    StandardizedData processJson(FileMetadata fileMetadata, FileFormat format);
}
