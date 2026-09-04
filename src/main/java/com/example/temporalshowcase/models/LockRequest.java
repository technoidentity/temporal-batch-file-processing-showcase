package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Duration;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LockRequest {
    private String lockKey;
    private Duration lockTimeout;
    private String requesterWorkflowId;
    private int priority;
}
