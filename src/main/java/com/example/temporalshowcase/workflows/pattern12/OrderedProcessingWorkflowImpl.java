package com.example.temporalshowcase.workflows.pattern12;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.DependencyActivities;
import com.example.temporalshowcase.activities.FileManagementActivities;
import com.example.temporalshowcase.models.FileMetadata;
import com.example.temporalshowcase.models.ProcessingResult;
import io.temporal.workflow.Workflow;

import java.util.List;

/**
 * Pattern 12 – Ordered Processing Workflow.
 *
 * Processing pipeline:
 *  1. enqueueReady     — file enters Ready Queue
 *  2. markProcessing   — file moves to Processing Queue
 *  3. processFileContent — actual processing (exclusive access)
 *  4. markCompleted / markFailed — file moves to final queue
 */
public class OrderedProcessingWorkflowImpl implements OrderedProcessingWorkflow {

    private int ready      = 0;
    private int processing = 0;
    private int completed  = 0;
    private int failed     = 0;

    private final DependencyActivities dependencyActivities = Workflow.newActivityStub(
            DependencyActivities.class,
            TemporalRuntime.activity("quick"));

    private final FileManagementActivities fileActivities = Workflow.newActivityStub(
            FileManagementActivities.class,
            TemporalRuntime.activity("file-long"));

    @Override
    public void processInOrder(List<FileMetadata> orderedFiles) {
        for (FileMetadata file : orderedFiles) {

            dependencyActivities.enqueueReady(file);
            ready++;

            dependencyActivities.markProcessing(file);
            ready--;
            processing++;

            try {
                ProcessingResult result = fileActivities.processFileContent(file);
                if ("FAILED".equals(result.getStatus())) {
                    dependencyActivities.markFailed(file, result.getMessage());
                    processing--;
                    failed++;
                } else {
                    dependencyActivities.markCompleted(file);
                    processing--;
                    completed++;
                }
            } catch (Exception e) {
                dependencyActivities.markFailed(file, e.getMessage());
                dependencyActivities.handleFileProcessingFailure(file, e.getMessage());
                processing--;
                failed++;
            }
        }
    }

    @Override
    public String getPipelineStatus() {
        return String.format("ready=%d processing=%d completed=%d failed=%d",
                ready, processing, completed, failed);
    }
}
