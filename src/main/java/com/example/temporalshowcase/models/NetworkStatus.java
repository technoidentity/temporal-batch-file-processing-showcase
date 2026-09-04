package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NetworkStatus {
    private boolean connected;
    private boolean stable;
    private long latencyMs;
    private double bandwidthMbps;
}
