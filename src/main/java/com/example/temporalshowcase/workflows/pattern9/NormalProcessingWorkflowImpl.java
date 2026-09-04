package com.example.temporalshowcase.workflows.pattern9;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.ResourceMonitorActivities;
import com.example.temporalshowcase.models.*;
import io.temporal.workflow.*;

import java.util.List;

/**
 * Pattern 9 – Normal Processing (Within Limits path).
 * Rate Limiter Service checked load and found it within safe thresholds.
 * Runs a quick resource check then processes files at full speed.
 */
public class NormalProcessingWorkflowImpl implements NormalProcessingWorkflow {

    private final ResourceMonitorActivities monitorActivities = Workflow.newActivityStub(
            ResourceMonitorActivities.class,
            TemporalRuntime.activity("monitor"));

    @Override
    public void executeNormalProcessing(int filesToProcess) {
        // Quick parallel resource snapshot to confirm within limits
        Promise<ResourceMetric> cpuPromise      = Async.function(monitorActivities::monitorCpuUsage);
        Promise<ResourceMetric> memPromise       = Async.function(monitorActivities::monitorMemoryUsage);
        Promise<ResourceMetric> diskPromise      = Async.function(monitorActivities::monitorDiskIO);
        Promise<ResourceMetric> netPromise       = Async.function(monitorActivities::monitorNetworkBandwidth);
        Promise<ResourceMetric> workflowsPromise = Async.function(monitorActivities::countActiveWorkflows);

        Promise.allOf(List.of(cpuPromise, memPromise, diskPromise, netPromise, workflowsPromise)).get();

        ResourceUsage usage = ResourceUsage.builder()
                .cpuUsage(cpuPromise.get())
                .memoryUsage(memPromise.get())
                .diskIO(diskPromise.get())
                .networkBandwidth(netPromise.get())
                .activeWorkflows(workflowsPromise.get())
                .build();

        // Load balancing strategy applied even in normal processing
        monitorActivities.applyLoadBalancing(usage, filesToProcess);

        // Process all files at full speed — no throttle needed
        monitorActivities.applyRateLimit(ThrottleRate.NORMAL);
    }
}
