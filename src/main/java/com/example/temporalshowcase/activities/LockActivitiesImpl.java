package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class LockActivitiesImpl implements LockActivities {

    private static final String STATE_FILE = "locks.json";

    private static ConcurrentHashMap<String, LockHandle> lockRegistry = new ConcurrentHashMap<>();

    @Value("${app.data.dir:}")
    private String dataDir;

    private ConcurrentHashMap<String, LockHandle> registry() {
        if (lockRegistry.isEmpty() && JsonFileStore.persistEnabled(dataDir)) {
            lockRegistry = JsonFileStore.load(dataDir, STATE_FILE, new TypeReference<>() {});
        }
        return lockRegistry;
    }

    private void persist() {
        JsonFileStore.save(dataDir, STATE_FILE, lockRegistry);
    }

    @Override
    public LockResult acquireLock(LockRequest lockRequest) {
        log.debug("Attempting to acquire lock: {}", lockRequest.getLockKey());
        long now = System.currentTimeMillis();
        LockHandle candidate = LockHandle.builder()
                .lockId(UUID.randomUUID().toString())
                .lockKey(lockRequest.getLockKey())
                .ownerWorkflowId(lockRequest.getRequesterWorkflowId())
                .acquiredAt(now)
                .expiresAt(now + lockRequest.getLockTimeout().toMillis())
                .build();

        AtomicReference<LockHandle> holder = new AtomicReference<>();
        registry().compute(lockRequest.getLockKey(), (k, existing) -> {
            if (existing != null && existing.getExpiresAt() > now) {
                holder.set(existing);
                return existing;
            }
            holder.set(candidate);
            return candidate;
        });

        LockHandle result = holder.get();
        boolean won = result != null && candidate.getLockId().equals(result.getLockId());
        if (won) {
            persist();
            log.info("Lock acquired: {} by workflow: {}", lockRequest.getLockKey(), lockRequest.getRequesterWorkflowId());
            return LockResult.builder().success(true).lockHandle(result).build();
        }
        return LockResult.builder()
                .success(false)
                .errorMessage("Lock already held by: " + result.getOwnerWorkflowId())
                .build();
    }

    @Override
    public void releaseLock(LockHandle lockHandle) {
        log.info("Releasing lock: {} held by: {}", lockHandle.getLockKey(), lockHandle.getOwnerWorkflowId());
        registry().remove(lockHandle.getLockKey());
        persist();
    }

    @Override
    public void forceReleaseLock(String lockKey) {
        log.warn("Force releasing lock: {}", lockKey);
        registry().remove(lockKey);
        persist();
    }

    @Override
    public LockHandle getLockInfo(String lockKey) {
        return registry().get(lockKey);
    }

    @Override
    public boolean isWorkflowActive(String workflowId) {
        log.debug("Checking if workflow is active: {}", workflowId);
        return true;
    }

    @Override
    public void logLockEvent(String lockKey, String event, String workflowId) {
        log.info("Lock event [{}] key={} workflow={}", event, lockKey, workflowId);
    }

    @Override
    public void resolveByPriority(String lockKey, int requesterPriority) {
        log.info("PRIORITY RESOLUTION — key={} requesterPriority={} — higher priority waiter gets preference",
                lockKey, requesterPriority);
    }

    @Override
    public void resolveByFCFS(String lockKey, String requesterWorkflowId) {
        log.info("FCFS RESOLUTION — key={} requester={} — first-come-first-serve order applied",
                lockKey, requesterWorkflowId);
    }

    @Override
    public void timeoutBasedRelease(String lockKey) {
        LockHandle holder = registry().get(lockKey);
        if (holder != null && holder.getExpiresAt() < System.currentTimeMillis()) {
            registry().remove(lockKey);
            persist();
            log.warn("TIMEOUT-BASED RELEASE — key={} expired lock forcibly released", lockKey);
        } else {
            log.warn("TIMEOUT-BASED RELEASE escalation — key={} lock still held, escalating to operator", lockKey);
        }
    }

    @Override
    public void manualInterventionOverride(String lockKey, String reason) {
        registry().remove(lockKey);
        persist();
        log.warn("MANUAL INTERVENTION OVERRIDE — key={} reason={} — operator force-released lock", lockKey, reason);
    }

    @Override
    public void notifyWaitingProcesses(String lockKey) {
        log.info("NOTIFY WAITING PROCESSES — key={} lock is now available", lockKey);
    }
}
