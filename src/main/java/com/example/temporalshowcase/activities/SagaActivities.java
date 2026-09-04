package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface SagaActivities {

    /** Copies the corrupted file to the quarantine directory and creates a placeholder if the source is missing. */
    @ActivityMethod
    void isolateCorruptedFile(FileMetadata fileMetadata);

    @ActivityMethod
    void alertOperationsTeam(FileMetadata fileMetadata, CorruptionDetails corruptionDetails);

    @ActivityMethod
    RepairResult attemptFileRepair(FileMetadata fileMetadata, CorruptionDetails corruptionDetails);

    @ActivityMethod
    ProcessingResult resumeProcessingAfterRepair(FileMetadata repairedFile);

    @ActivityMethod
    void requestFileResend(FileMetadata fileMetadata);

    /** Compensation: removes the .repaired copy to restore the original file state. */
    @ActivityMethod
    void restoreFileState(FileMetadata fileMetadata);

    @ActivityMethod
    void rollbackDatabaseTransactions(FileMetadata fileMetadata);

    @ActivityMethod
    void cancelDownstreamProcessing(FileMetadata fileMetadata);

    @ActivityMethod
    void notifyConsumersOfFailure(FileMetadata fileMetadata, String reason);

    @ActivityMethod
    void logCompensationExecution(Compensation compensation, String status);

    /** Step 7: Mark the file processing case as resolved after compensation. */
    @ActivityMethod
    void updateProcessingStatus(FileMetadata fileMetadata, String status, String reason);
}
