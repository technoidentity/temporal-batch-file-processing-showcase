package com.example.temporalshowcase.models;

public enum ProcessingStatus {
    PENDING,
    IN_PROGRESS,
    COMPLETED,
    FAILED,
    SKIPPED,
    WAITING_FOR_DEPENDENCIES
}
