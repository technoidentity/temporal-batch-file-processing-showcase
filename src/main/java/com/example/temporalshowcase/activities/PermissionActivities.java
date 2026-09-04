package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface PermissionActivities {

    @ActivityMethod
    boolean checkFilePermissions(FileMetadata fileMetadata);

    @ActivityMethod
    void logPermissionIssue(FileMetadata fileMetadata, PermissionIssue permissionIssue);

    @ActivityMethod
    AutoFixResult applyStandardPermissions(FileMetadata fileMetadata, PermissionIssue issue);

    @ActivityMethod
    AutoFixResult requestTemporaryAccess(FileMetadata fileMetadata, PermissionIssue issue);

    @ActivityMethod
    AutoFixResult createServiceAccount(FileMetadata fileMetadata, PermissionIssue issue);

    @ActivityMethod
    AutoFixResult updateFileOwnership(FileMetadata fileMetadata, PermissionIssue issue);

    @ActivityMethod
    EscalationTicket escalateToSecurityTeam(FileMetadata fileMetadata, PermissionIssue issue);

    @ActivityMethod
    String checkTicketStatus(String ticketId);

    @ActivityMethod
    void escalateToSeniorManagement(FileMetadata fileMetadata, EscalationTicket ticket);

    @ActivityMethod
    ProcessingResult resumeProcessingWithPermission(FileMetadata fileMetadata);

    /** Human Intervention — Security Team Reviews the ticket */
    @ActivityMethod
    void notifySecurityTeamReview(EscalationTicket ticket);

    /** Human Intervention — System Admin approves the permission change */
    @ActivityMethod
    void requestSystemAdminApproval(FileMetadata fileMetadata, EscalationTicket ticket);

    /** Human Intervention — Business Owner confirms access is legitimate */
    @ActivityMethod
    void requestBusinessOwnerConfirmation(FileMetadata fileMetadata, EscalationTicket ticket);

    /** Human Intervention — Document the full permission escalation for audit trail */
    @ActivityMethod
    void documentAuditTrail(FileMetadata fileMetadata, PermissionIssue issue, String resolution);
}
