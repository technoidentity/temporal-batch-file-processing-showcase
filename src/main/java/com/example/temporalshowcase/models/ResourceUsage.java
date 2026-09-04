package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ResourceUsage {
    private ResourceMetric cpuUsage;
    private ResourceMetric memoryUsage;
    private ResourceMetric diskIO;
    private ResourceMetric networkBandwidth;
    private ResourceMetric activeWorkflows;
}
