# Batch File Integrations — Status, Coverage & Confidence

**Date:** 2026-09-04
**Stack:** Java 17 · Spring Boot 3.2.5 · Temporal Java SDK 1.24.1 · Maven
**Scope:** 12 SFG batch-file operational patterns from *Batch File Integrations Operational challenges.pdf*, exposed as a REST showcase over Temporal.

---

## 1. Executive summary

All 12 patterns are implemented with real orchestration logic and real file I/O
(against `data/incoming`), backed by an automated **18-test time-skipping suite**
(`src/test/.../PatternsWorkflowTest.java`) that exercises every pattern plus the
failure branches.

This build is configured and named for handover:

- **Fully externalized configuration** — every Temporal and business knob (task
  queues, retry/timeout policies, SLAs, circuit-breaker thresholds, chunk sizes,
  lock/transfer/rate-limit/validation/dependency parameters) lives in
  `application.yml` as typed `@ConfigurationProperties`. **Nothing is hardcoded**
  in workflow or activity code.
- **Runs anywhere by config alone** — local Temporal, the bundled Docker stack,
  or **Temporal Cloud** (TLS + API-key auth) with no code change.
- **Resilient worker** — `ShowcaseWorker` registers and the app boots even when
  Temporal is unreachable, then connects in the background.
- **Real SFTP** — Pattern 1 has a real SFG poller (`SftpFilePoller`: SFTP GET →
  `fileArrived` signal); Pattern 8 does real byte-range SFTP transfers resuming
  from checkpoint offset. Both fall back to local mode when SFTP is disabled.
- **Meaningful naming** — workflow **types** use business names (`Pnn-…`, shown
  in the UI); workflow **IDs** are pattern-numbered (`pNN-…`), including child
  workflows — no raw UUID-only IDs.
- **Correct SLA semantics** — the Pattern 1 SLA timer runs in a
  `CancellationScope` and is explicitly cancelled on file arrival; the SLA
  duration is configurable (`temporal.sla.file-arrival`).
- **Durable state** — circuit-breaker / idempotency / lock state is file-backed
  under `data/state/*.json` and survives a restart.

**Overall confidence: 9 / 10** as a *pattern showcase and reference
implementation*. Remaining items are production-hardening (cluster state store,
workflow versioning) — documented in §5, none blocking the demo.

---

## 2. Per-pattern coverage & confidence

Legend — Real: activity does genuine work (file I/O / algorithm). Sim: simulated. Test: automated test present.

| # | Pattern | Workflow type | Test | Key Temporal features | Real vs Sim | Confidence |
|---|---------|---------------|:----:|-----------------------|-------------|:----------:|
| 1 | Delayed files / signal + SLA | `P01-FileProcessing` | ✅ signal + timeout | Signal, Query, `await`, cancellable SLA timer, heartbeat, ApplicationFailure | Real file read; **real SFTP poller** (`SftpFilePoller`) | 9.5 |
| 2 | Large files / chunked | `P02-LargeFileChunking` (+`P02-ChunkProcessor`) | ✅ | Child workflows (`Async`+`Promise.allOf`), `ParentClosePolicy.ABANDON`, heartbeats | Real byte-range reads | 9 |
| 3 | Empty files / circuit breaker | `P03-EmptyFileCircuitBreaker` | ✅ empty + window | Query, last-N failure rate, fast-fail | Real read; **durable under data/state/** | 9 |
| 4 | Corrupted / saga | `P04-CorruptedFileSaga` | ✅ | LIFO compensations, quarantine, repair | Real quarantine/repair; hand-rolled saga | 8 |
| 5 | Duplicate / idempotency | `P05-DuplicateFileIdempotency` | ✅ first + duplicate | Child workflow, SHA-256 key, atomic CAS claim | Real hash; **durable under data/state/** | 9 |
| 6 | Format mismatch / router | `P06-MultiFormatRouter` (+ per-format children) | ✅ CSV route | 4 child processor workflows, detection pipeline | Real content inspection | 9 |
| 7 | Permission / escalation | `P07-PermissionEscalation` | ✅ signal + update + timeout | Signal, **Update**, Query, bounded `await`, tiered escalation | Sim (errorCode-driven) | 8.5 |
| 8 | Network / resumable transfer | `P08-NormalTransfer`, `P08-ResumableTransfer` | ✅ normal transfer | Checkpoints (JSON), recovery loop, integrity check | **Real byte-range SFTP** (or local); failure is sim-flag | 8.5 |
| 9 | Resource exhaustion / rate limit | `P09-AdaptiveRateLimiting`, `P09-NormalProcessing` | ✅ | Parallel monitor activities, weighted load, throttle tiers | **OS MXBean / disk / thread metrics**; bounded loop | 8 |
| 10 | Data quality / multi-stage | `P10-DataQualityValidation` | ✅ ok + escalation | 4-stage state machine, remediation loop, escalate | Real file checks; ML fix stubbed | 8 |
| 11 | Concurrent access / locking | `P11-DistributedFileLock`, `P11-LockWait` | ✅ lock-acquired | Child `LockWaitWorkflow`, backoff, guaranteed release | Atomic CAS lock; **durable under data/state/** (single-host) | 8 |
| 12 | File ordering / dependency graph | `P12-DependencyGraph`, `P12-OrderedProcessing`, `P12-DependencyWait` | ✅ no-cycles + cycle | DFS cycle detect, Kahn topo-sort, child workflows | Real graph algo; wait is sim | 8 |

**Automated tests (18):** P1 signal + P1 SLA-timeout; P2 chunked; P3 empty + P3 sliding window; P4 saga; P5 first + duplicate; P6 CSV; P7 signal + P7 update + P7 timeout; P8 normal transfer; P9 rate-limit; P10 quality-ok + P10 escalation; P11 lock-acquired; P12 no-cycles + P12 cycle.
Scenarios not unit-tested (P8 resumable/integrity, P11 wait/timeout contention, P12 dependency-wait) are demoable via [`PATTERN_CURLS.md`](PATTERN_CURLS.md) and share code with the tested paths.

---

## 3. Configuration surface (`application.yml`)

All of the following is externalized under `temporal.*` — nothing hardcoded:

| Section | Purpose |
|---------|---------|
| `connection` | endpoint, namespace, TLS, API key, identity (local / Docker / Temporal Cloud) |
| `queues` | logical name → Temporal task-queue (one per pattern, 14 total) |
| `policies` | named activity retry/timeout policies (start-to-close, heartbeat, retry) |
| `sla.file-arrival` | Pattern 1 SLA wait for the `fileArrived` signal (default 24h) |
| `patterns.<pattern>` | per-pattern business knobs (chunk sizes, circuit thresholds, transfer segment, lock/rate-limit/validation/dependency parameters) |
| `sftp` | Pattern 1 poller + Pattern 8 transfer SFTP settings (host, port, credentials, remote dir, `enabled`) |

Env overrides exist for the values that vary per environment: `TEMPORAL_TARGET`,
`TEMPORAL_NAMESPACE`, `TEMPORAL_TLS`, `TEMPORAL_API_KEY`, `SERVER_PORT`,
`APP_DATA_DIR`, `SLA_FILE_ARRIVAL`, and the `SFTP_*` keys. Full details and the
per-pattern knob table are in the project [`README.md`](../README.md).

---

## 4. Temporal best-practices scorecard

| Practice | Status |
|----------|--------|
| Deterministic workflows (no wall-clock/random/IO in workflow code) | ✅ uses `Workflow.currentTimeMillis()` / `Workflow.newRandom()` |
| Interface + impl split, per-pattern task queues | ✅ 14 queues (from config) |
| Signals / Queries | ✅ all patterns queried; signals on P1, P7 |
| Updates | ✅ P7 `resolvePermission` |
| Child workflows (meaningful IDs) | ✅ P2, P5, P6, P9, P11, P12 — all `pNN-…` |
| Cancellable timers | ✅ P1 SLA timer in a `CancellationScope`, cancelled on arrival |
| Saga compensation | ✅ P4 (hand-rolled; see §5) |
| Typed `ApplicationFailure` errors | ✅ across patterns |
| Activity heartbeats on long activities | ✅ P1/P2 file activities |
| Bounded human/await timeouts | ✅ P7; P1 SLA bounded + configurable |
| Automated tests (`TestWorkflowEnvironment`) | ✅ 18 |
| Externalized configuration (no hardcoded knobs) | ✅ all under `temporal.*` in `application.yml` |
| Temporal Cloud support | ✅ TLS + API-key by config |
| Worker starts irrespective of Temporal availability | ✅ background connect |
| Configurable namespace | ✅ `temporal.connection.namespace` |
| Durable cross-restart state | ✅ JSON under `data/state/` (single-host; not a cluster store) |
| `ParentClosePolicy` set explicitly | ⚠️ P2 only (others use SDK default) |
| ContinueAsNew for long loops | ❌ P9 monitor loop bounded by `max-wait-cycles` instead |
| `Workflow.getVersion` versioning | ❌ not used (greenfield) |

---

## 5. What's missing / recommended next (prioritized)

None block the showcase; these raise it to production-grade.

| Pri | Item | Pattern(s) | Effort | Note |
|-----|------|-----------|--------|------|
| P1 | Redis/Postgres for circuit / idempotency / lock (shared across hosts) | 3, 5, 11 | M | JSON under `data/state/` survives restart on one host; not a cluster store |
| P1 | Real distributed lock (one-workflow-per-resource **or** store with fencing tokens) | 11 | M | acquire is atomic CAS + persisted; still single-host |
| P2 | `ContinueAsNew` for the rate-limit monitor loop | 9 | S | keeps history bounded for a continuous limiter |
| P2 | Unit-test the P8 resume + integrity-restart branches | 8 | S | currently only the normal path is unit-tested |
| P3 | Use `io.temporal.workflow.Saga` instead of the hand-rolled compensation list | 4 | S | idiomatic; automatic reverse-order compensation |
| P3 | Introduce `Workflow.getVersion` scaffolding + a versioning policy | all | S | required before editing any deployed workflow body |
| P4 | Bump Temporal SDK (1.24.1 → current) and server image (`temporalio/temporal:1.8.2` is old) | infra | S | security/features |

---

## 6. Project layout

```
batch_file_integrations/
├── pom.xml, mvnw, mvnw.cmd, .mvn/       # build (Maven wrapper)
├── docker-compose.yml, Dockerfile        # temporal + app + sftp stack
├── README.md
├── src/main/java/com/example/temporalshowcase/
│   ├── config/       TemporalProperties, TemporalRuntime, TemporalConfig (client)
│   ├── worker/       ShowcaseWorker (starts irrespective of Temporal)
│   ├── controller/   ShowcaseController (REST start/query/signal endpoints)
│   ├── sftp/         SftpGateway, SftpFilePoller (Pattern 1)
│   ├── workflows/    pattern1 … pattern12
│   └── activities/   real file / validation / transfer / lock work
├── src/main/resources/application.yml    # all Temporal + business + SFTP config
├── src/test/java/...                     # PatternsWorkflowTest (18 tests)
├── data/{incoming,outgoing,quarantine,checkpoints,state}
├── sftp_data/incoming/                   # SFTP polled dir (Pattern 1)
├── docs/
│   ├── STATUS.md                         # this file
│   └── PATTERN_CURLS.md                  # per-pattern curl scenarios
└── scripts/
    └── sftp-stage / sftp-restore (.sh + .cmd)   # move a file into / out of the polled dir
```

### Quick start
```bash
docker compose up --build        # full stack: App :8080 · Temporal UI :8233 · SFTP :2222
# or, against your own Temporal dev server:
./mvnw spring-boot:run
./mvnw test                       # 18 time-skipping tests, no server needed
```
