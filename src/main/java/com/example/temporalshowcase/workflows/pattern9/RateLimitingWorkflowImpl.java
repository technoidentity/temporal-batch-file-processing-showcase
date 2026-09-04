package com.example.temporalshowcase.workflows.pattern9;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.ResourceMonitorActivities;
import com.example.temporalshowcase.models.*;
import io.temporal.workflow.*;

import java.time.Duration;
import java.util.List;

/**
 * Pattern 9: Adaptive Throttling (Rate Limit Exceeded path).
 *
 * Throttle loop per monitoring cycle:
 *  1. Monitor Resource Usage         — 5 metrics gathered IN PARALLEL via Promise.allOf()
 *  2. Calculate Current Load Factor  — weighted formula (CPU 30%, Mem 25%, Disk 20%, Net 15%, WF 10%)
 *  3. Determine Throttle Rate        — NORMAL / LIGHT / MODERATE / HEAVY / CRITICAL
 *  4. Apply Rate Limit / Queue files — applyRateLimit() + queueFilesWithPriority() + optimizeResourceAllocation()
 *  5. Wait for Resource Availability — Workflow.sleep() + re-check loop
 *     ├─ Resources Available → 6. Resume Processing / Release throttled files
 *     └─ Still Exhausted    → 7. Escalate / Alert operations
 */
public class RateLimitingWorkflowImpl implements RateLimitingWorkflow {

    private final double   HIGH_LOAD_THRESHOLD         = TemporalRuntime.patterns().getRateLimiting().getHighLoadThreshold();
    private final double   CRITICAL_LOAD_THRESHOLD     = TemporalRuntime.patterns().getRateLimiting().getCriticalLoadThreshold();
    private final Duration MONITORING_INTERVAL         = TemporalRuntime.patterns().getRateLimiting().getMonitoringInterval();
    private final Duration WAIT_FOR_RESOURCES_INTERVAL = TemporalRuntime.patterns().getRateLimiting().getWaitForResourcesInterval();
    private final int      MAX_WAIT_CYCLES             = TemporalRuntime.patterns().getRateLimiting().getMaxWaitCycles();

    private final ResourceMonitorActivities monitorActivities = Workflow.newActivityStub(
            ResourceMonitorActivities.class,
            TemporalRuntime.activity("monitor"));

    // @QueryMethod state
    private String throttleStatus = "STARTING";

    @Override
    public void manageRateLimiting(int maxCycles) {
        for (int cycle = 0; cycle < maxCycles; cycle++) {

            // ── Step 1: Monitor Resource Usage — all 5 metrics IN PARALLEL ──
            throttleStatus = "MONITORING cycle=" + (cycle + 1) + "/" + maxCycles;
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

            // ── Step 2: Calculate Current Load Factor ──
            double load = calculateLoadFactor(usage);

            // ── Step 3: Determine Throttle Rate ──
            ThrottleRate rate = determineThrottleRate(load);
            throttleStatus = String.format("%s load=%.2f cycle=%d/%d", rate.name(), load, cycle + 1, maxCycles);

            if (load >= HIGH_LOAD_THRESHOLD) {
                // ── Step 4: Apply Rate Limit / Queue Files ──
                monitorActivities.applyRateLimit(rate);
                monitorActivities.queueFilesWithPriority(usage, rate);
                monitorActivities.optimizeResourceAllocation(usage);

                // ── Step 5: Wait for Resource Availability ──
                throttleStatus = String.format("WAITING_FOR_RESOURCES throttle=%s load=%.2f", rate.name(), load);
                boolean resourcesFreed = false;

                for (int waitCycle = 0; waitCycle < MAX_WAIT_CYCLES; waitCycle++) {
                    Workflow.sleep(WAIT_FOR_RESOURCES_INTERVAL);

                    Promise<ResourceMetric> rCpu  = Async.function(monitorActivities::monitorCpuUsage);
                    Promise<ResourceMetric> rMem  = Async.function(monitorActivities::monitorMemoryUsage);
                    Promise<ResourceMetric> rDisk = Async.function(monitorActivities::monitorDiskIO);
                    Promise<ResourceMetric> rNet  = Async.function(monitorActivities::monitorNetworkBandwidth);
                    Promise<ResourceMetric> rWf   = Async.function(monitorActivities::countActiveWorkflows);
                    Promise.allOf(List.of(rCpu, rMem, rDisk, rNet, rWf)).get();

                    ResourceUsage recheck = ResourceUsage.builder()
                            .cpuUsage(rCpu.get()).memoryUsage(rMem.get()).diskIO(rDisk.get())
                            .networkBandwidth(rNet.get()).activeWorkflows(rWf.get())
                            .build();
                    double recheckLoad = calculateLoadFactor(recheck);

                    if (recheckLoad < HIGH_LOAD_THRESHOLD) {
                        // ── Step 6: Resources Available → Resume Processing / Release throttled files ──
                        throttleStatus = String.format("RESOURCES_AVAILABLE load=%.2f — releasing files", recheckLoad);
                        monitorActivities.releaseThrottledFiles(rate);
                        monitorActivities.resumeProcessingAfterThrottle(recheck);
                        monitorActivities.applyLoadBalancing(recheck, 0);
                        resourcesFreed = true;
                        break;
                    }
                }

                if (!resourcesFreed) {
                    // ── Step 7: Still Exhausted → Escalate / Alert operations ──
                    throttleStatus = String.format("STILL_EXHAUSTED load=%.2f — escalating", load);
                    monitorActivities.escalateResourceExhaustion(usage);
                }

            } else {
                monitorActivities.applyRateLimit(rate);
                monitorActivities.applyLoadBalancing(usage, 0);
                NormalProcessingWorkflow normal = Workflow.newChildWorkflowStub(
                        NormalProcessingWorkflow.class,
                        ChildWorkflowOptions.newBuilder()
                                .setTaskQueue(TemporalRuntime.queue("rate-limiting"))
                                .setWorkflowId("normal-proc-" + Workflow.getInfo().getWorkflowId() + "-c" + cycle)
                                .build());
                normal.executeNormalProcessing(0);
            }

            if (cycle < maxCycles - 1) {
                Workflow.sleep(MONITORING_INTERVAL);
            }
        }
        throttleStatus = "COMPLETED — all " + maxCycles + " cycles done";
    }

    /**
     * Weighted composite load metric: CPU 30%, Memory 25%, Disk 20%, Network 15%, Active Workflows 10%.
     * Value is in [0.0, 1.0]; thresholds at 0.80 (high) and 0.95 (critical).
     */
    private double calculateLoadFactor(ResourceUsage usage) {
        return (usage.getCpuUsage().getUtilization()         * 0.30)
             + (usage.getMemoryUsage().getUtilization()      * 0.25)
             + (usage.getDiskIO().getUtilization()           * 0.20)
             + (usage.getNetworkBandwidth().getUtilization() * 0.15)
             + (usage.getActiveWorkflows().getUtilization()  * 0.10);
    }

    /**
     * Maps composite load factor to a ThrottleRate tier.
     * Boundaries: &lt;0.50 NORMAL, &lt;0.70 LIGHT, &lt;0.85 MODERATE, &lt;0.95 HEAVY, ≥0.95 CRITICAL.
     */
    private ThrottleRate determineThrottleRate(double loadFactor) {
        if (loadFactor < 0.50) return ThrottleRate.NORMAL;
        if (loadFactor < 0.70) return ThrottleRate.LIGHT;
        if (loadFactor < 0.85) return ThrottleRate.MODERATE;
        if (loadFactor < 0.95) return ThrottleRate.HEAVY;
        return ThrottleRate.CRITICAL;
    }

    @Override
    public String getCurrentThrottleStatus() {
        return throttleStatus;
    }
}
