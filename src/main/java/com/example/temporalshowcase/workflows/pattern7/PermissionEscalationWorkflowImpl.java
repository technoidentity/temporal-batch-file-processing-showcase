package com.example.temporalshowcase.workflows.pattern7;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.PermissionActivities;
import com.example.temporalshowcase.models.*;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * Pattern 7: Permission Escalation.
 *
 * Flow:
 *   Permission Check Service
 *     ├─ Permission OK   → Normal Processing Workflow
 *     └─ Permission Denied → Permission Resolution:
 *           1. Log Permission Issue (Security audit trail)
 *           2. Attempt Auto Fix (4 strategies):
 *                Apply Standard Permissions / Request Temp Access /
 *                Create Service Account / Update File Ownership
 *              ├─ Auto Fix Success → 3. Resume Processing
 *              └─ Auto Fix Failed  → 4. Escalate to Security (create ticket)
 *                                  → 5. Wait for Resolution (human intervention):
 *                                         Security Team Review
 *                                         System Admin Approval
 *                                         Business Owner Confirmation
 *                                         Audit Trail Documentation
 *                                  → 6. Re-check Permissions
 *                                       ├─ Permission OK   → 3. Resume Processing
 *                                       └─ Still Denied    → 7. Escalate Further (senior mgmt)
 */
public class PermissionEscalationWorkflowImpl implements PermissionEscalationWorkflow {

    /** Bounded wait for human intervention — the PDF lists "configurable timeouts"
     *  as a pro, so the escalation must not wait forever. */
    private final Duration HUMAN_RESOLUTION_TIMEOUT = TemporalRuntime.patterns().getPermission().getHumanResolutionTimeout();

    private volatile String humanResolution = null;
    private String escalationStatus = "STARTING";

    private final PermissionActivities permissionActivities = Workflow.newActivityStub(
            PermissionActivities.class,
            TemporalRuntime.activity("permission"));

    // ──────────────────────────────────────────────────────────────────────────

    @Override
    public ProcessingResult resolvePermissionIssue(FileMetadata fileMetadata, PermissionIssue permissionIssue) {

        // ── Permission Check Service ──────────────────────────────────────────
        escalationStatus = "CHECKING";
        boolean hasPermission = permissionActivities.checkFilePermissions(fileMetadata);

        if (hasPermission) {
            escalationStatus = "PERMISSION_OK";
            return permissionActivities.resumeProcessingWithPermission(fileMetadata);
        }

        // ── Permission Denied → Permission Resolution workflow ────────────────

        escalationStatus = "LOGGING";
        permissionActivities.logPermissionIssue(fileMetadata, permissionIssue);

        escalationStatus = "AUTO_FIX";
        AutoFixResult autoFixResult = attemptAutoFix(fileMetadata, permissionIssue);

        if (autoFixResult.isSuccess()) {
            escalationStatus = "RESOLVED_AUTO";
            permissionActivities.documentAuditTrail(fileMetadata, permissionIssue,
                    "AUTO_FIX: " + autoFixResult.getStrategyApplied());
            return permissionActivities.resumeProcessingWithPermission(fileMetadata);
        }

        escalationStatus = "ESCALATING_SECURITY";
        EscalationTicket ticket = permissionActivities.escalateToSecurityTeam(fileMetadata, permissionIssue);

        // Step 5: Wait for Resolution — only unblocked by the permissionResolved signal
        escalationStatus = "WAITING_HUMAN";
        permissionActivities.notifySecurityTeamReview(ticket);
        permissionActivities.requestSystemAdminApproval(fileMetadata, ticket);
        permissionActivities.requestBusinessOwnerConfirmation(fileMetadata, ticket);

        boolean resolvedInTime = Workflow.await(HUMAN_RESOLUTION_TIMEOUT, () -> humanResolution != null);
        if (!resolvedInTime) {
            escalationStatus = "ESCALATING_SENIOR_TIMEOUT";
            permissionActivities.documentAuditTrail(fileMetadata, permissionIssue,
                    "HUMAN_RESOLUTION_TIMEOUT after " + HUMAN_RESOLUTION_TIMEOUT + " — escalating to senior management");
            permissionActivities.escalateToSeniorManagement(fileMetadata, ticket);
            throw ApplicationFailure.newFailure(
                    "No human resolution within " + HUMAN_RESOLUTION_TIMEOUT + " for: " + fileMetadata.getFileName(),
                    "PERMISSION_ESCALATION_TIMEOUT");
        }

        // Pass HUMAN_RESOLVED flag so checkFilePermissions knows the human actually fixed it
        escalationStatus = "RECHECKING";
        FileMetadata resolvedMeta = FileMetadata.builder()
                .filePath(fileMetadata.getFilePath())
                .fileName(fileMetadata.getFileName())
                .fileSize(fileMetadata.getFileSize())
                .fileType("HUMAN_RESOLVED")
                .sourceSystem(fileMetadata.getSourceSystem())
                .build();
        boolean permissionOk = permissionActivities.checkFilePermissions(resolvedMeta);

        if (!permissionOk) {
            escalationStatus = "ESCALATING_SENIOR";
            permissionActivities.documentAuditTrail(fileMetadata, permissionIssue, "STILL_DENIED — escalating to senior management");
            permissionActivities.escalateToSeniorManagement(fileMetadata, ticket);
            throw ApplicationFailure.newFailure(
                    "Permissions still denied after human resolution", "PERMISSION_DENIED");
        }

        escalationStatus = "RESOLVED_HUMAN";
        permissionActivities.documentAuditTrail(fileMetadata, permissionIssue,
                "RESOLVED by human intervention: " + humanResolution);
        return permissionActivities.resumeProcessingWithPermission(fileMetadata);
    }

    @Override
    public void permissionResolved(String resolution) {
        this.humanResolution = resolution;
    }

    @Override
    public String resolvePermission(String resolution) {
        this.humanResolution = resolution;
        return "ACK — resolution recorded: " + resolution;
    }

    @Override
    public String getEscalationStatus() {
        return escalationStatus;
    }

    // ── Auto Fix Strategies ───────────────────────────────────────────────────

    /**
     * Tries four strategies in order: standard permissions → temporary access →
     * service account → file ownership update. Returns the first success or
     * a failure if all four are blocked (e.g. STRICT error code).
     */
    private AutoFixResult attemptAutoFix(FileMetadata fileMetadata, PermissionIssue issue) {
        List<java.util.function.BiFunction<FileMetadata, PermissionIssue, AutoFixResult>> strategies = Arrays.asList(
                permissionActivities::applyStandardPermissions,
                permissionActivities::requestTemporaryAccess,
                permissionActivities::createServiceAccount,
                permissionActivities::updateFileOwnership);

        for (var strategy : strategies) {
            try {
                AutoFixResult result = strategy.apply(fileMetadata, issue);
                if (result.isSuccess()) return result;
            } catch (Exception ignored) {
                // continue to next strategy
            }
        }
        return AutoFixResult.failure("All auto-fix strategies failed");
    }
}
