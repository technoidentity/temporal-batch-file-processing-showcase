package com.example.temporalshowcase.workflows.pattern6;

import com.example.temporalshowcase.models.FileFormat;
import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.StandardizedData;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface XmlProcessorWorkflow {
    @WorkflowMethod(name = "P06-XmlProcessor")
    StandardizedData processXml(FileMetadata fileMetadata, FileFormat format);
}
