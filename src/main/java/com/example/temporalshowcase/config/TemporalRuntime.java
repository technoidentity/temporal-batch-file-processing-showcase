package com.example.temporalshowcase.config;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.ChildWorkflowOptions;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Static accessor over {@link TemporalProperties} so deterministic workflow code
 * can build ActivityOptions / ChildWorkflowOptions and resolve task-queue names
 * from configuration (application.yml) rather than hardcoded values. The values
 * are constant for the life of the process, so reading them during a workflow is
 * replay-safe.
 *
 * Populated once at startup by Spring; tests call {@link #init(TemporalProperties)}.
 */
@Component
public class TemporalRuntime {

    private static volatile TemporalProperties props;

    public TemporalRuntime(TemporalProperties properties) {
        init(properties);
    }

    /** Test hook: initialise the static holder without a Spring context. */
    public static void init(TemporalProperties properties) {
        props = properties;
    }

    private static TemporalProperties props() {
        TemporalProperties p = props;
        if (p == null) {
            throw new IllegalStateException("TemporalRuntime not initialised — no TemporalProperties bound");
        }
        return p;
    }

    /** Resolves a logical queue key (e.g. "idempotency") to its task-queue name. */
    public static String queue(String key) {
        String q = props().getQueues().get(key);
        if (q == null) {
            throw new IllegalStateException("No task queue configured for key '" + key + "' (temporal.queues in application.yml)");
        }
        return q;
    }

    /** Builds ActivityOptions from a named policy (temporal.policies.<key>). */
    public static ActivityOptions activity(String policyKey) {
        TemporalProperties.Policy p = policy(policyKey);
        ActivityOptions.Builder b = ActivityOptions.newBuilder()
                .setStartToCloseTimeout(p.getStartToCloseTimeout())
                .setRetryOptions(retry(p.getRetry()));
        if (p.getHeartbeatTimeout() != null && !p.getHeartbeatTimeout().isZero()) {
            b.setHeartbeatTimeout(p.getHeartbeatTimeout());
        }
        return b.build();
    }

    /** ActivityOptions from a named policy, pinned to a specific task queue. */
    public static ActivityOptions activityOnQueue(String policyKey, String queueKey) {
        return ActivityOptions.newBuilder(activity(policyKey))
                .setTaskQueue(queue(queueKey))
                .build();
    }

    /** Child-workflow options: task queue by key + optional execution timeout. */
    public static ChildWorkflowOptions child(String queueKey, Duration executionTimeout) {
        ChildWorkflowOptions.Builder b = ChildWorkflowOptions.newBuilder().setTaskQueue(queue(queueKey));
        if (executionTimeout != null && !executionTimeout.isZero()) {
            b.setWorkflowExecutionTimeout(executionTimeout);
        }
        return b.build();
    }

    public static TemporalProperties.Policy policy(String key) {
        TemporalProperties.Policy p = props().getPolicies().get(key);
        if (p == null) {
            throw new IllegalStateException("No policy configured for key '" + key + "' (temporal.policies in application.yml)");
        }
        return p;
    }

    public static TemporalProperties.Sftp sftp() {
        return props().getSftp();
    }

    /** Pattern 1 default SLA (wait time for the fileArrived signal). */
    public static java.time.Duration fileArrivalSla() {
        return props().getSla().getFileArrival();
    }

    /** Per-pattern business knobs (thresholds, timeouts, attempts, sizes). */
    public static TemporalProperties.Patterns patterns() {
        return props().getPatterns();
    }

    private static RetryOptions retry(TemporalProperties.Retry r) {
        RetryOptions.Builder b = RetryOptions.newBuilder()
                .setInitialInterval(r.getInitialInterval())
                .setBackoffCoefficient(r.getBackoffCoefficient())
                .setMaximumInterval(r.getMaximumInterval());
        if (r.getMaximumAttempts() > 0) {
            b.setMaximumAttempts(r.getMaximumAttempts());
        }
        return b.build();
    }
}
