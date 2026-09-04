package com.example.temporalshowcase.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * All Temporal + integration configuration, bound from application.yml under the
 * {@code temporal.*} prefix. Nothing here is hardcoded in the workflow/activity
 * code — connection, task queues, retry/timeout policies and SFTP settings all
 * come from configuration so the same build runs locally, in Docker, or against
 * Temporal Cloud purely by changing yml / environment variables.
 */
@Data
@ConfigurationProperties(prefix = "temporal")
public class TemporalProperties {

    private Connection connection = new Connection();
    /** Logical queue key -> Temporal task-queue name. */
    private Map<String, String> queues = new LinkedHashMap<>();
    /** Named activity policy -> timeouts + retry. */
    private Map<String, Policy> policies = new LinkedHashMap<>();
    private Sftp sftp = new Sftp();
    private Sla sla = new Sla();
    private Patterns patterns = new Patterns();

    @Data
    public static class Sla {
        /** Pattern 1 default SLA: how long to wait for the fileArrived signal. */
        private Duration fileArrival = Duration.ofHours(24);
    }

    /** Per-pattern business knobs (thresholds, timeouts, attempts, sizes). */
    @Data
    public static class Patterns {
        private LargeFile largeFile = new LargeFile();
        private CircuitBreaker circuitBreaker = new CircuitBreaker();
        private Idempotency idempotency = new Idempotency();
        private Permission permission = new Permission();
        private Transfer transfer = new Transfer();
        private RateLimiting rateLimiting = new RateLimiting();
        private Validation validation = new Validation();
        private Locking locking = new Locking();
        private Dependency dependency = new Dependency();
    }

    @Data
    public static class LargeFile {
        private DataSize maxChunk = DataSize.ofMegabytes(100);
        private DataSize minChunk = DataSize.ofMegabytes(10);
    }

    @Data
    public static class CircuitBreaker {
        private Duration recoveryTimeout = Duration.ofMinutes(5);
        private int windowSize = 10;
        private double failureRateThreshold = 0.50;
        private int minSamples = 3;
    }

    @Data
    public static class Idempotency {
        private Duration waitPollInterval = Duration.ofSeconds(30);
        private Duration inProgressTimeout = Duration.ofHours(2);
    }

    @Data
    public static class Permission {
        private Duration humanResolutionTimeout = Duration.ofHours(4);
    }

    @Data
    public static class Transfer {
        private DataSize segment = DataSize.ofKilobytes(50);
        private Duration networkRecoveryTimeout = Duration.ofMinutes(30);
        private Duration baseNetworkPollInterval = Duration.ofSeconds(30);
        private int maxRestartAttempts = 3;
        private Duration maxWorkflowTimeout = Duration.ofHours(2);
        private int maxNetworkRecoveryAttempts = 5;
    }

    @Data
    public static class RateLimiting {
        private double highLoadThreshold = 0.80;
        private double criticalLoadThreshold = 0.95;
        private Duration monitoringInterval = Duration.ofSeconds(30);
        private Duration waitForResourcesInterval = Duration.ofSeconds(15);
        private int maxWaitCycles = 4;
    }

    @Data
    public static class Validation {
        private int maxRemediationAttempts = 3;
    }

    @Data
    public static class Locking {
        private Duration lockTimeout = Duration.ofMinutes(30);
        private int maxLockAttempts = 3;
        private int waitMaxAttempts = 4;
        private Duration waitMaxTimeout = Duration.ofSeconds(25);
        private Duration waitBaseBackoff = Duration.ofSeconds(3);
    }

    @Data
    public static class Dependency {
        private Duration pollInterval = Duration.ofSeconds(3);
        private int pollsUntilReady = 2;
        private int maxPolls = 8;
    }

    @Data
    public static class Connection {
        /** gRPC endpoint. Local: 127.0.0.1:7233. Cloud: <region>.<cloud>.api.temporal.io:7233 */
        private String target = "127.0.0.1:7233";
        /** Local: "default". Cloud: "<namespace>.<accountId>". */
        private String namespace = "default";
        /** Enable TLS — required for Temporal Cloud. */
        private boolean tls = false;
        /** Temporal Cloud API key (blank locally). */
        private String apiKey = "";
        /** Worker identity suffix (optional). */
        private String identity = "";
    }

    @Data
    public static class Policy {
        private Duration startToCloseTimeout = Duration.ofMinutes(5);
        /** Optional; null/zero means no heartbeat timeout. */
        private Duration heartbeatTimeout;
        private Retry retry = new Retry();
    }

    @Data
    public static class Retry {
        private Duration initialInterval = Duration.ofSeconds(1);
        private double backoffCoefficient = 2.0;
        private Duration maximumInterval = Duration.ofSeconds(60);
        /** 0 = unlimited (Temporal default); set a bound to fail fast. */
        private int maximumAttempts = 0;
    }

    @Data
    public static class Sftp {
        /** When true, the Pattern-1 poller connects to the SFTP server and drives fileArrived. */
        private boolean enabled = false;
        private String host = "localhost";
        private int port = 2222;
        private String username = "batch";
        private String password = "batch";
        /** Remote directory the poller watches. */
        private String remoteDir = "incoming";
        /** Local directory downloaded files are written to (SFTP GET target). */
        private String downloadDir = "./data/incoming";
        /** How often the poller lists the remote directory. */
        private Duration pollInterval = Duration.ofSeconds(10);
        /** SLA passed to the workflow when the poller starts it. */
        private Duration sla = Duration.ofHours(24);
    }
}
