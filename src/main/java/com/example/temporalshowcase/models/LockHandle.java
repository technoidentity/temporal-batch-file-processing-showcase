package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LockHandle {
    private String lockId;
    private String lockKey;
    private String ownerWorkflowId;
    private long acquiredAt;
    private long expiresAt;
}
