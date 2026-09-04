package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface TransferActivities {

    @ActivityMethod
    NetworkStatus checkNetworkConnectivity();

    /** Simulation-aware connectivity check — returns DISCONNECTED for the first N calls when simulateNetworkFailureCount > 0. */
    @ActivityMethod
    NetworkStatus checkNetworkConnectivityWithSimulation(TransferRequest request);

    @ActivityMethod
    TransferResult executeTransferSegment(TransferState transferState);

    @ActivityMethod
    void saveTransferCheckpoint(CheckpointData checkpointData);

    @ActivityMethod
    CheckpointData loadTransferCheckpoint(String filePath);

    @ActivityMethod
    boolean validateTransferIntegrity(TransferState transferState);

    @ActivityMethod
    void updateTransferProgress(TransferState transferState);

    /** Step 1 – detect and record that a transfer interruption has occurred. */
    @ActivityMethod
    String detectTransferInterruption(TransferState transferState);
}
