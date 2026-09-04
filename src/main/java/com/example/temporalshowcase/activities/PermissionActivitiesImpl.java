package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
public class PermissionActivitiesImpl implements PermissionActivities {

    /**
     * Permission check simulation:
     * - Known clean files (payment_batch, daily_trades, allowed_*)  → always GRANTED
     * - fileType == "HUMAN_RESOLVED"  → GRANTED  (workflow sets this after signal received)
     * - Everything else               → DENIED
     */
    @Override
    public boolean checkFilePermissions(FileMetadata fileMetadata) {
        String name = fileMetadata.getFileName() != null ? fileMetadata.getFileName() : "";
        String fileType = fileMetadata.getFileType() != null ? fileMetadata.getFileType() : "";

        if (name.startsWith("allowed_")
                || name.equals("payment_batch_2026.csv")
                || name.equals("daily_trades_20260831.csv")) {
            log.info("Permission check for {}: GRANTED (known clean file)", name);
            return true;
        }
        if ("HUMAN_RESOLVED".equals(fileType)) {
            log.info("Permission check for {}: GRANTED (human resolved)", name);
            return true;
        }
        log.info("Permission check for {}: DENIED", name);
        return false;
    }

    @Override
    public void logPermissionIssue(FileMetadata fileMetadata, PermissionIssue permissionIssue) {
        log.warn("Permission issue logged - file: {} required: {} current: {}",
                fileMetadata.getFilePath(), permissionIssue.getRequiredPermission(), permissionIssue.getCurrentPermission());
    }

    @Override
    public AutoFixResult applyStandardPermissions(FileMetadata fileMetadata, PermissionIssue issue) {
        log.info("Applying standard permissions to: {}", fileMetadata.getFilePath());
        // STRICT scenario: all auto-fixes fail so human intervention is required
        if ("STRICT".equals(issue.getErrorCode())) return AutoFixResult.failure("Standard permissions not allowed");
        return AutoFixResult.success("APPLY_STANDARD_PERMISSIONS");
    }

    @Override
    public AutoFixResult requestTemporaryAccess(FileMetadata fileMetadata, PermissionIssue issue) {
        log.info("Requesting temporary access for: {}", fileMetadata.getFilePath());
        if ("STRICT".equals(issue.getErrorCode())) return AutoFixResult.failure("Temporary access not allowed");
        return AutoFixResult.success("TEMPORARY_ACCESS");
    }

    @Override
    public AutoFixResult createServiceAccount(FileMetadata fileMetadata, PermissionIssue issue) {
        log.info("Creating service account for: {}", fileMetadata.getFilePath());
        if ("STRICT".equals(issue.getErrorCode())) return AutoFixResult.failure("Service account creation blocked");
        return AutoFixResult.success("SERVICE_ACCOUNT_CREATED");
    }

    @Override
    public AutoFixResult updateFileOwnership(FileMetadata fileMetadata, PermissionIssue issue) {
        log.info("Updating file ownership for: {}", fileMetadata.getFilePath());
        if ("STRICT".equals(issue.getErrorCode())) return AutoFixResult.failure("Ownership update not permitted");
        return AutoFixResult.success("OWNERSHIP_UPDATED");
    }

    @Override
    public EscalationTicket escalateToSecurityTeam(FileMetadata fileMetadata, PermissionIssue issue) {
        String ticketId = "SEC-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        log.info("Escalating to security team - ticket: {} file: {}", ticketId, fileMetadata.getFilePath());
        return EscalationTicket.builder()
                .ticketId(ticketId)
                .filePath(fileMetadata.getFilePath())
                .assignedTeam("SECURITY")
                .priority("HIGH")
                .createdAt(System.currentTimeMillis())
                .status("OPEN")
                .build();
    }

    @Override
    public String checkTicketStatus(String ticketId) {
        log.debug("Checking status of ticket: {} — returning OPEN (waiting for human signal)", ticketId);
        return "OPEN";  // workflow parks here until permissionResolved signal arrives
    }

    @Override
    public void escalateToSeniorManagement(FileMetadata fileMetadata, EscalationTicket ticket) {
        log.warn("Escalating to senior management - file: {} original ticket: {}", fileMetadata.getFilePath(), ticket.getTicketId());
    }

    @Override
    public ProcessingResult resumeProcessingWithPermission(FileMetadata fileMetadata) {
        log.info("Resuming processing after permission resolution: {}", fileMetadata.getFilePath());
        return ProcessingResult.builder()
                .status(ProcessingStatus.COMPLETED)
                .message("Processing resumed after permission fix")
                .build();
    }

    @Override
    public void notifySecurityTeamReview(EscalationTicket ticket) {
        log.info("SECURITY TEAM REVIEW — ticket={} priority={} assigned={}",
                ticket.getTicketId(), ticket.getPriority(), ticket.getAssignedTeam());
    }

    @Override
    public void requestSystemAdminApproval(FileMetadata fileMetadata, EscalationTicket ticket) {
        log.info("SYSTEM ADMIN APPROVAL REQUEST — file={} ticket={}",
                fileMetadata.getFileName(), ticket.getTicketId());
    }

    @Override
    public void requestBusinessOwnerConfirmation(FileMetadata fileMetadata, EscalationTicket ticket) {
        log.info("BUSINESS OWNER CONFIRMATION REQUEST — file={} ticket={}",
                fileMetadata.getFileName(), ticket.getTicketId());
    }

    @Override
    public void documentAuditTrail(FileMetadata fileMetadata, PermissionIssue issue, String resolution) {
        log.info("AUDIT TRAIL DOCUMENTED — file={} requiredPermission={} resolution={}",
                fileMetadata.getFileName(), issue.getRequiredPermission(), resolution);
    }
}
