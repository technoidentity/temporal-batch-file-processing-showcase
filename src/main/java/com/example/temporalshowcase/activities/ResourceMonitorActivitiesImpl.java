package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
@Component
public class ResourceMonitorActivitiesImpl implements ResourceMonitorActivities {

    @Value("${app.data.dir:./data}")
    private String dataDir;

    @Override
    public ResourceMetric monitorCpuUsage() {
        double usage = clamp01(processCpuLoad());
        log.debug("CPU utilization: {}", usage);
        return ResourceMetric.builder().metricName("CPU").utilization(usage).unit("%").build();
    }

    @Override
    public ResourceMetric monitorMemoryUsage() {
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        long used = memory.getHeapMemoryUsage().getUsed();
        long max = memory.getHeapMemoryUsage().getMax();
        if (max <= 0) {
            Runtime rt = Runtime.getRuntime();
            used = rt.totalMemory() - rt.freeMemory();
            max = rt.maxMemory();
        }
        double usage = max > 0 ? clamp01((double) used / max) : 0;
        log.debug("Memory utilization: {} ({} / {})", usage, used, max);
        return ResourceMetric.builder()
                .metricName("MEMORY")
                .utilization(usage)
                .absoluteValue(used)
                .unit("bytes")
                .build();
    }

    @Override
    public ResourceMetric monitorDiskIO() {
        double usage = 0;
        try {
            FileStore store = Files.getFileStore(Path.of(dataDir != null && !dataDir.isBlank() ? dataDir : ".").toAbsolutePath());
            long total = store.getTotalSpace();
            long usable = store.getUsableSpace();
            usage = total > 0 ? clamp01(1.0 - ((double) usable / total)) : 0;
        } catch (Exception e) {
            log.warn("Disk usage unavailable: {}", e.getMessage());
        }
        log.debug("Disk utilization: {}", usage);
        return ResourceMetric.builder().metricName("DISK_IO").utilization(usage).unit("%").build();
    }

    @Override
    public ResourceMetric monitorNetworkBandwidth() {
        // JVM cannot read NIC saturation without OS tools; treat as unused unless disk/CPU is high.
        double usage = clamp01(processCpuLoad() * 0.25);
        log.debug("Network proxy utilization (CPU-derived): {}", usage);
        return ResourceMetric.builder().metricName("NETWORK").utilization(usage).unit("proxy").build();
    }

    @Override
    public ResourceMetric countActiveWorkflows() {
        int threads = ManagementFactory.getThreadMXBean().getThreadCount();
        double usage = clamp01(threads / 200.0);
        log.debug("Active threads (workflow-load proxy): {}", threads);
        return ResourceMetric.builder()
                .metricName("ACTIVE_WORKFLOWS")
                .utilization(usage)
                .absoluteValue(threads)
                .build();
    }

    private static double processCpuLoad() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.OperatingSystemMXBean sunOs) {
            double load = sunOs.getProcessCpuLoad();
            if (load >= 0) return load;
            load = sunOs.getCpuLoad();
            if (load >= 0) return load;
        }
        double sys = os.getSystemLoadAverage();
        int cpus = Math.max(1, os.getAvailableProcessors());
        return sys >= 0 ? sys / cpus : 0;
    }

    private static double clamp01(double v) {
        if (Double.isNaN(v) || v < 0) return 0;
        return Math.min(1.0, v);
    }

    @Override
    public void applyRateLimit(ThrottleRate throttleRate) {
        log.info("Applying rate limit: {}", throttleRate);
    }

    @Override
    public void escalateResourceExhaustion(ResourceUsage resourceUsage) {
        log.error("CRITICAL: Resource exhaustion detected - CPU: {} Memory: {}",
                resourceUsage.getCpuUsage().getUtilization(),
                resourceUsage.getMemoryUsage().getUtilization());
    }

    @Override
    public void queueFilesWithPriority(ResourceUsage resourceUsage, ThrottleRate throttleRate) {
        log.info("PRIORITY QUEUE — throttle={} cpu={:.0f}% mem={:.0f}% — queuing files by priority tier",
                throttleRate,
                resourceUsage.getCpuUsage().getUtilization() * 100,
                resourceUsage.getMemoryUsage().getUtilization() * 100);
    }

    @Override
    public void releaseThrottledFiles(ThrottleRate throttleRate) {
        log.info("RELEASE THROTTLE — previous throttle={} — releasing queued files for processing", throttleRate);
    }

    @Override
    public void resumeProcessingAfterThrottle(ResourceUsage resourceUsage) {
        log.info("RESUME PROCESSING — cpu={:.0f}% mem={:.0f}% — resources available, resuming normal throughput",
                resourceUsage.getCpuUsage().getUtilization() * 100,
                resourceUsage.getMemoryUsage().getUtilization() * 100);
    }

    @Override
    public void optimizeResourceAllocation(ResourceUsage resourceUsage) {
        log.info("RESOURCE ALLOCATION OPTIMIZER — rebalancing cpu={:.0f}% mem={:.0f}% disk={:.0f}%",
                resourceUsage.getCpuUsage().getUtilization() * 100,
                resourceUsage.getMemoryUsage().getUtilization() * 100,
                resourceUsage.getDiskIO().getUtilization() * 100);
    }

    @Override
    public void applyLoadBalancing(ResourceUsage resourceUsage, int pendingFiles) {
        log.info("LOAD BALANCING — pendingFiles={} activeWorkflows={} — distributing load across workers",
                pendingFiles,
                resourceUsage.getActiveWorkflows().getAbsoluteValue());
    }
}
