package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CorruptionDetails {
    private String corruptionType;
    private long corruptedOffset;
    private String expectedChecksum;
    private String actualChecksum;
    private String description;
}
