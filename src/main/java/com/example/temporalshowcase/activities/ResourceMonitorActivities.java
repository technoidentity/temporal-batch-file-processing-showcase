package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface ResourceMonitorActivities {

    @ActivityMethod
    ResourceMetric monitorCpuUsage();

    @ActivityMethod
    ResourceMetric monitorMemoryUsage();

    @ActivityMethod
    ResourceMetric monitorDiskIO();

    @ActivityMethod
    ResourceMetric monitorNetworkBandwidth();

    @ActivityMethod
    ResourceMetric countActiveWorkflows();

    @ActivityMethod
    void applyRateLimit(ThrottleRate throttleRate);

    @ActivityMethod
    void escalateResourceExhaustion(ResourceUsage resourceUsage);

    /** Step 4 – Adaptive Controls: queue incoming files respecting priority tiers. */
    @ActivityMethod
    void queueFilesWithPriority(ResourceUsage resourceUsage, ThrottleRate throttleRate);

    /** Step 6 – Adaptive Controls: release throttled files and resume processing. */
    @ActivityMethod
    void releaseThrottledFiles(ThrottleRate throttleRate);

    /** Step 6 – resume normal processing after throttle is lifted. */
    @ActivityMethod
    void resumeProcessingAfterThrottle(ResourceUsage resourceUsage);

    /** Adaptive Controls: optimize resource allocation across workflows. */
    @ActivityMethod
    void optimizeResourceAllocation(ResourceUsage resourceUsage);

    /** Adaptive Controls: apply load balancing strategy. */
    @ActivityMethod
    void applyLoadBalancing(ResourceUsage resourceUsage, int pendingFiles);
}
