# Batch File Integrations — Temporal Reference Implementation

A Spring Boot service implementing twelve Temporal patterns for SFG (Secure File
Gateway) batch-file operational challenges. Each pattern is a self-contained
Temporal workflow with its own task queue, exposed over a small REST API and
observable in the Temporal Web UI.

All Temporal configuration (connection, task queues, retry/timeout policies,
SFTP) is externalized to `src/main/resources/application.yml` — nothing is
hardcoded in the workflow or activity code. The service runs against a local
Temporal, the bundled Docker stack, or Temporal Cloud by configuration alone.

---

## Architecture

```
  client / curl
     │  POST start · POST signal · GET query
     ▼
  ┌───────────────────────┐   start / signal / query    ┌────────────────────────┐
  │ ShowcaseController     │ ──────────────────────────▶ │  Temporal Server       │
  │ (/api/showcase/**)     │                             │  local / Docker / Cloud│
  └──────────┬────────────┘                             └───────────┬────────────┘
             │ watch(workflowId, fileName)                          │ task-queue polling
             ▼                                                      │  (workers poll here)
  ┌───────────────────────┐                                        │
  │ FileArrivalMonitor    │  sees data/incoming/<file> created     │
  │ (watches data/incoming│ ── then sends fileArrived signal ─────▶│
  └───────────────────────┘                                        │
  ┌───────────────────────┐  SFTP GET, then start + fileArrived    │
  │ SftpFilePoller (P1)    │ ──────────────────────────────────────▶│
  │ (when SFTP enabled)    │                                        │
  └───────────────────────┘                                        ▼
                                                       ┌────────────────────────┐
  ┌───────────────────────┐   workflows invoke         │ ShowcaseWorker (app)    │
  │ Activities (file I/O)  │ ◀───────────────────────── │ hosts 12 workflow types │
  │ FileManagement, Saga,  │   activities               │ + activity impls on 14  │
  │ CircuitBreaker,        │ ─────────────────────────▶ │ task queues; starts once│
  │ Transfer, Lock, …      │   return results           │ Temporal is reachable   │
  └───────────────────────┘                            └────────────────────────┘

  Configuration (externalized in application.yml)
    TemporalProperties ──▶ TemporalRuntime ──▶ deterministic workflow / activity code
                       └──▶ TemporalConfig  ──▶ lazy WorkflowClient + WorkerFactory
```

Flow: the REST controller (or the SFTP poller) starts a workflow on Temporal;
signals arrive from `/send-signal`, the `FileArrivalMonitor` (local file watcher),
or the `SftpFilePoller`; the `ShowcaseWorker` polls the 14 task queues, runs the
deterministic workflow code, and invokes the activities that do the real file I/O.

- **Configuration** — `TemporalProperties` binds everything under `temporal.*`
  from `application.yml`; `TemporalRuntime` exposes it to deterministic
  workflow code; `TemporalConfig` builds a lazy `WorkflowClient` and
  `WorkerFactory` so the app starts even when Temporal is unreachable.
- **REST API** — `ShowcaseController` (`/api/showcase/**`) starts workflows and
  exposes signal/query endpoints.
- **File arrival triggers** — `FileArrivalMonitor` watches `data/incoming/` for
  the `/start-waiting` flow; `SftpFilePoller` performs real SFTP GETs and
  signals `fileArrived` when SFTP is enabled.
- **Workers** — `ShowcaseWorker` registers all workflow and activity
  implementations on 14 task queues (one per pattern plus chunk/retry queues)
  and begins polling in a background thread once Temporal is reachable.
- **Workflows** — deterministic, no I/O; child workflows are used for chunking,
  idempotency, locking, format processing, dependency waiting, etc.
- **Activities** — real file/validation/transfer/lock/graph work under
  `activities/`.

---

## Prerequisites

- Java 17
- Maven (or the bundled `./mvnw` wrapper)
- Docker + Docker Compose (for the bundled Temporal + SFTP stack)

---

## Running

### Option A — Docker (Temporal + app + SFTP)

```bash
docker compose up --build
```

| Service | URL |
| --- | --- |
| App API | http://localhost:8080 |
| Health | http://localhost:8080/actuator/health |
| Temporal UI | http://localhost:8233 |
| SFTP | localhost:2222 (user `batch` / `batch`) |

### Option B — Local (app against an existing Temporal)

Start a Temporal dev server (`temporal server start-dev`), then:

```bash
./mvnw spring-boot:run
```

The worker comes up even if Temporal is down and connects once it is reachable.

### Configuration

Everything is overridable via environment variables (see `application.yml`):

| Setting | Env var | Default |
| --- | --- | --- |
| Temporal endpoint | `TEMPORAL_TARGET` | `127.0.0.1:7233` |
| Namespace | `TEMPORAL_NAMESPACE` | `default` |
| TLS | `TEMPORAL_TLS` | `false` |
| API key | `TEMPORAL_API_KEY` | *(empty)* |
| Worker identity | `TEMPORAL_IDENTITY` | *(empty)* |
| App port | `SERVER_PORT` | `8080` |
| Data dir | `APP_DATA_DIR` | `./data` |
| Pattern 1 SLA | `SLA_FILE_ARRIVAL` | `24h` |
| SFTP enabled | `SFTP_ENABLED` | `false` |
| SFTP host / port | `SFTP_HOST` / `SFTP_PORT` | `localhost` / `2222` |
| SFTP credentials | `SFTP_USER` / `SFTP_PASSWORD` | `batch` / `batch` |
| SFTP remote dir | `SFTP_REMOTE_DIR` | `incoming` |
| SFTP download dir | `SFTP_DOWNLOAD_DIR` | `./data/incoming` |
| SFTP poll interval | `SFTP_POLL_INTERVAL` | `10s` |
| SFTP SLA | `SFTP_SLA` | `24h` |

**Configuration philosophy.** All Temporal and business configuration lives in
`application.yml` as typed, nested `@ConfigurationProperties` (bound into
`TemporalProperties` and exposed to workflows via `TemporalRuntime`). Nothing —
task queues, retry/timeout policies, SLAs, circuit-breaker thresholds, chunk
sizes, lock/transfer/rate-limit knobs — is hardcoded in the workflow or activity
code. Only the handful of values that vary per environment are surfaced as
`${ENV:default}` overrides (endpoint, namespace, TLS, API key, identity, ports,
SFTP, default SLA). This keeps structured config (e.g. per-policy objects,
per-pattern maps) readable and type-safe rather than as dozens of flat environment
variables.

Full `application.yml` structure under `temporal.*`:

| Section | Purpose |
| --- | --- |
| `connection` | endpoint, namespace, TLS, API key, identity (local or Temporal Cloud) |
| `queues` | logical name → Temporal task-queue (one per pattern) |
| `policies` | named activity retry/timeout policies (start-to-close, heartbeat, retry) |
| `sla.file-arrival` | Pattern 1 **default** SLA wait for the `fileArrived` signal (24h) |
| `patterns.<pattern>` | per-pattern business knobs — see below |
| `sftp` | Pattern 1 poller + Pattern 8 transfer SFTP settings (includes its own `sla`) |

Per-pattern knobs (`temporal.patterns.*`), all tunable without code changes.
YAML uses kebab-case keys (e.g. `large-file`) that Spring binds to the camelCase
fields in `TemporalProperties`:

| Pattern | Keys |
| --- | --- |
| `large-file` (P2) | `max-chunk`, `min-chunk` |
| `circuit-breaker` (P3) | `recovery-timeout`, `window-size`, `failure-rate-threshold`, `min-samples` |
| `idempotency` (P5) | `wait-poll-interval`, `in-progress-timeout` |
| `permission` (P7) | `human-resolution-timeout` |
| `transfer` (P8) | `segment`, `network-recovery-timeout`, `base-network-poll-interval`, `max-restart-attempts`, `max-workflow-timeout`, `max-network-recovery-attempts` |
| `rate-limiting` (P9) | `high-load-threshold`, `critical-load-threshold`, `monitoring-interval`, `wait-for-resources-interval`, `max-wait-cycles` |
| `validation` (P10) | `max-remediation-attempts` |
| `locking` (P11) | `lock-timeout`, `max-lock-attempts`, `wait-max-attempts`, `wait-max-timeout`, `wait-base-backoff` |
| `dependency` (P12) | `poll-interval`, `polls-until-ready`, `max-polls` |

#### Two SLA settings — how they differ

Both default to `24h` and both express the same idea (how long a Pattern 1
workflow waits for the `fileArrived` signal before failing with `SLA_TIMEOUT`),
but they apply to **different entry points**:

| Key (env) | Applies to | Meaning |
| --- | --- | --- |
| `temporal.sla.file-arrival` (`SLA_FILE_ARRIVAL`) | REST-started Pattern 1 workflows | The **default** deadline used by `FileProcessingWorkflow` when the caller does not pass its own `slaTimeoutSeconds`. The `/pattern1/timeout-test?slaSeconds=` endpoint overrides it per request. |
| `temporal.sftp.sla` (`SFTP_SLA`) | Workflows started by the **SFTP poller** | The deadline `SftpFilePoller` stamps on each workflow it starts for a file it discovers over SFTP. Because the poller downloads the file and signals `fileArrived` immediately after starting, this is effectively a safety-net deadline — kept separate so the SFTP-triggered path can differ from the REST default. |

### Temporal Cloud

Set the connection to your Cloud namespace and API key — no code change:

```bash
export TEMPORAL_TARGET=<region>.<cloud>.api.temporal.io:7233
export TEMPORAL_NAMESPACE=<namespace>.<accountId>
export TEMPORAL_API_KEY=<api-key>
export TEMPORAL_TLS=true
./mvnw spring-boot:run
```

The client enables TLS and sends `Authorization: Bearer <key>` plus the
`temporal-namespace` routing header.

---

## Starting workflows

All start endpoints are `POST`; status endpoints are `GET`. Most start calls need
no body and use the sample files under `data/incoming/`. Workflow IDs are
pattern-numbered (`p01…p12`); workflow **types** (shown in the UI) use business
names.

| # | Use case | Workflow type | Start endpoint(s) | Status query |
|---|----------|---------------|-------------------|--------------|
| 1 | Delayed files / SLA | `P01-FileProcessing` | `POST /api/showcase/pattern1/signal-arrival` · `/start-waiting` · `/send-signal/{id}` · `/timeout-test` | `GET /api/showcase/pattern1/status/{id}` |
| 2 | Large files / chunking | `P02-LargeFileChunking` (+`P02-ChunkProcessor`) | `POST /api/showcase/pattern2/large-file` | `GET /api/showcase/pattern2/progress/{id}` |
| 3 | Empty files / circuit breaker | `P03-EmptyFileCircuitBreaker` | `POST /api/showcase/pattern3/circuit-breaker` | `GET /api/showcase/pattern3/circuit-state/{id}` |
| 4 | Corrupted files / saga | `P04-CorruptedFileSaga` | `POST /api/showcase/pattern4/saga-recovery` | `GET /api/showcase/pattern4/saga-status/{id}` |
| 5 | Duplicate files / idempotency | `P05-DuplicateFileIdempotency` | `POST /api/showcase/pattern5/idempotency` · `/duplicate` | `GET /api/showcase/pattern5/dedup-state/{id}?fileHash=` |
| 6 | Format mismatches | `P06-MultiFormatRouter` (+ per-format) | `POST /api/showcase/pattern6/multi-format?format=CSV\|XML\|JSON\|FIXED_WIDTH` | `GET /api/showcase/pattern6/format/{id}` |
| 7 | Permission issues | `P07-PermissionEscalation` | `POST /api/showcase/pattern7/scenario1-permission-ok` · `/scenario2-autofix` · `/scenario3-human-intervention` · `/scenario4-resolve/{id}` | `GET /api/showcase/pattern7/escalation-status/{id}` |
| 8 | Network / resumable transfer | `P08-NormalTransfer`, `P08-ResumableTransfer` | `POST /api/showcase/pattern8/normal-transfer` · `/resumable-transfer` · `/integrity-failure` | `GET /api/showcase/pattern8/progress/{id}` |
| 9 | Resource exhaustion / rate limiting | `P09-AdaptiveRateLimiting`, `P09-NormalProcessing` | `POST /api/showcase/pattern9/within-limits` · `/rate-exceeded` | `GET /api/showcase/pattern9/throttle-status/{id}` |
| 10 | Data quality | `P10-DataQualityValidation` | `POST /api/showcase/pattern10/quality-ok` · `/business-remediation` · `/needs-remediation` | `GET /api/showcase/pattern10/stage/{id}` |
| 11 | Concurrent access / locking | `P11-DistributedFileLock`, `P11-LockWait` | `POST /api/showcase/pattern11/lock-acquired` · `/lock-wait-acquired` · `/lock-wait-timeout` | `GET /api/showcase/pattern11/lock-status/{id}` · `/lock-wait-status/{id}` |
| 12 | File ordering dependencies | `P12-DependencyGraph`, `P12-OrderedProcessing`, `P12-DependencyWait` | `POST /api/showcase/pattern12/no-cycles` · `/dependency-wait` · `/cycle-resolution` | `GET /api/showcase/pattern12/stage/{id}` |

Example:

```bash
# start
curl -X POST http://localhost:8080/api/showcase/pattern1/signal-arrival
# → {"workflowId":"p01-signal-...", ...}

# query
curl http://localhost:8080/api/showcase/pattern1/status/<workflowId>
```

The full set of curl scenarios is in [`docs/PATTERN_CURLS.md`](docs/PATTERN_CURLS.md).
Implementation status and test coverage are summarized in [`docs/STATUS.md`](docs/STATUS.md).

---

## Pattern scenarios (what each one does)

Every scenario below is executed automatically by `scripts/run-patterns.sh`
(interactive menu, or `scripts/run-patterns.sh all` / `... 1 8 11` / `... 1,3,8-10`),
which POSTs the start endpoint, polls the status query, and reports the HTTP result.
The descriptions match that script step for step so you can follow along or drive
the endpoints by hand.

### Pattern 1 — Delayed files / signal + SLA · `P01-FileProcessing`

The workflow starts and waits for a `fileArrived` signal, bounded by an SLA timer
(cancelled the moment the file arrives). The signal can come from the REST API, the
local `FileArrivalMonitor`, or the real `SftpFilePoller`.

- **Scenario 1 — start + signal together.** `POST /pattern1/signal-arrival` starts
  the workflow and immediately sends `fileArrived`. Status → `COMPLETED`.
- **Scenario 2 — start-waiting, then send-signal.** `POST /pattern1/start-waiting?fileName=…`
  leaves it in `WAITING_FOR_FILE_SIGNAL`; `POST /pattern1/send-signal/{id}?fileName=…`
  delivers the signal → `PROCESSING` → `COMPLETED`.
- **Scenario 3 — local file monitor.** After `start-waiting`, `touch data/incoming/<file>`;
  the `FileArrivalMonitor` detects the newly created/touched file and signals the
  workflow (no manual `send-signal` needed).
- **Scenario 4 — SLA timeout.** `POST /pattern1/timeout-test?slaSeconds=8` starts a
  workflow whose file never arrives; the SLA timer fires and the workflow fails with
  `SLA_TIMEOUT` (status goes `WAITING_FOR_FILE_SIGNAL` → `SLA_VIOLATED` → `FAILED`).
- **Scenario 5 — real SFTP poller.** Stage a file into the SFTP dir
  (`scripts/sftp-stage.sh <file>`); the poller performs an SFTP GET, starts
  `p01-sftp-<file>`, and signals `fileArrived`. Requires the SFTP container and
  `temporal.sftp.enabled=true`. See [Pattern 1 — SFTP mode](#pattern-1--sftp-mode-real-sfg-poller).

### Pattern 2 — Large files / chunking · `P02-LargeFileChunking` (+ `P02-ChunkProcessor`)

- **Single scenario — chunked processing.** `POST /pattern2/large-file` splits the
  file into chunks (bounded by `patterns.large-file.max-chunk`/`min-chunk`) and
  processes them via child workflows (`Async` + `Promise.allOf`). Query
  `GET /pattern2/progress/{id}`. The bundled sample is under the 100 MB threshold, so
  progress reads `1/1 chunks`.

### Pattern 3 — Empty files / circuit breaker · `P03-EmptyFileCircuitBreaker`

Empty-vs-content is decided by bytes on disk. The breaker keeps a sliding window of
the last N files (`patterns.circuit-breaker.window-size`) and opens when the empty
rate exceeds the threshold with at least `min-samples`. State is durable under
`data/state/`.

- **Scenario 1 — file has content (CLOSED).** `POST /pattern3/circuit-breaker` with a
  non-empty file → processed normally, circuit `CLOSED`.
- **Scenario 2 — empty file ×3 (trip OPEN).** POST an empty file three times; the
  empty rate crosses 50 % with ≥3 samples → circuit `OPEN`.
- **Scenario 3 — content file while OPEN (blocked).** With the circuit open, a
  content file fails fast with `CIRCUIT_BREAKER_OPEN` until the recovery window
  (`recovery-timeout`) elapses and a `HALF_OPEN` probe is allowed.

### Pattern 4 — Corrupted files / saga · `P04-CorruptedFileSaga`

- **Scenario 1 — corrupted file.** `POST /pattern4/saga-recovery` on a file with
  corruption markers → isolate → alert → repair → resume. Query `/pattern4/saga-status/{id}`.
- **Scenario 2 — clean file.** A valid file skips the saga and processes normally.
- **Scenario 3 — repair fails (source missing).** With the source file removed,
  repair fails → request resend → LIFO compensations run in reverse → `RESOLVED`.

### Pattern 5 — Duplicate files / idempotency · `P05-DuplicateFileIdempotency`

- **Scenario 1 — first send.** `POST /pattern5/idempotency` hashes the file (SHA-256),
  claims it atomically, and processes it.
- **Scenario 2 — duplicate send.** `POST /pattern5/duplicate` sends the same file;
  the hash matches, so processing is skipped and the cached result is returned.
- **Scenario 3 — query dedup state.** `GET /pattern5/dedup-state/{id}?fileHash=<sha256>`
  reports `CHECKING` / `DUPLICATE_RETURNED` / etc. Dedup state is durable under `data/state/`.

### Pattern 6 — Format mismatches / router · `P06-MultiFormatRouter` (+ per-format children)

- **Scenarios (one per format).** `POST /pattern6/multi-format?format=CSV|XML|JSON|FIXED_WIDTH`
  detects the format and routes to the matching child processor workflow. Query
  `GET /pattern6/format/{id}`. Sample files: `records.csv/.xml/.json`, `large_transaction_dump.dat`.

### Pattern 7 — Permission / escalation · `P07-PermissionEscalation`

- **Scenario 1 — permission already OK.** `POST /pattern7/scenario1-permission-ok` →
  normal processing.
- **Scenario 2 — auto-fix succeeds.** `POST /pattern7/scenario2-autofix` → an
  automatic remediation strategy restores access and processing continues.
- **Scenario 3 + 4 — human intervention then resolve.** `POST /pattern7/scenario3-human-intervention`
  (errorCode `STRICT`) makes every auto-fix fail, so the workflow waits (bounded by
  `patterns.permission.human-resolution-timeout`, default 4h) for a human;
  `POST /pattern7/scenario4-resolve/{id}?resolution=…` sends the `permissionResolved`
  signal and processing continues. If nobody resolves it in time it escalates with
  `PERMISSION_ESCALATION_TIMEOUT`. (An `@UpdateMethod` variant also exists.)

### Pattern 8 — Network / resumable transfer · `P08-NormalTransfer`, `P08-ResumableTransfer`

- **Scenario 1 — normal transfer (network OK).** `POST /pattern8/normal-transfer` →
  single-shot transfer, no checkpointing (`p08-normal-…`).
- **Scenario 2 — network failure → checkpoint → resume.** `POST /pattern8/resumable-transfer`
  simulates the first two connectivity checks failing → saves a checkpoint → waits →
  resumes from the byte offset → validates integrity → completes. Query `/pattern8/progress/{id}`.
- **Scenario 3 — integrity failure → restart.** `POST /pattern8/integrity-failure`
  completes the transfer but fails the first integrity check → restarts from byte 0 →
  passes → completes. When SFTP is enabled, segments are real byte-range SFTP GETs.

### Pattern 9 — Resource exhaustion / rate limiting · `P09-AdaptiveRateLimiting`, `P09-NormalProcessing`

- **Scenario 1 — within limits.** `POST /pattern9/within-limits?filesToProcess=10` →
  load is low, files process without throttling (`p09-normal-…`, no throttle query).
- **Scenario 2 — rate exceeded.** `POST /pattern9/rate-exceeded?cycles=3` runs adaptive
  throttling across the monitoring cycles (weighted CPU/disk/thread load → throttle
  tiers). Query `GET /pattern9/throttle-status/{id}`.

### Pattern 10 — Data quality / multi-stage · `P10-DataQualityValidation`

Four sequential gates: BASIC → BUSINESS → ADVANCED → FINAL, with a remediation loop
bounded by `patterns.validation.max-remediation-attempts`.

- **Scenario 1 — all stages pass.** `POST /pattern10/quality-ok` on a clean file →
  all four gates pass → `COMPLETE`.
- **Scenario 2 — business remediation.** `POST /pattern10/business-remediation` on a
  file that fails business rules → `correctMissingData` → approved → `COMPLETE`.
- **Scenario 3 — advanced fails → escalate.** `POST /pattern10/needs-remediation` on a
  file with corruption markers → advanced validation fails and remediation is retried
  up to the limit → escalated for manual review (`VALIDATION_ESCALATED`).
  Query `GET /pattern10/stage/{id}`.

### Pattern 11 — Concurrent access / locking · `P11-DistributedFileLock`, `P11-LockWait`

Lock state is durable under `data/state/` (single-host).

- **Scenario 1 — lock acquired (no contention).** `POST /pattern11/lock-acquired` →
  acquire → process → release. Query `/pattern11/lock-status/{id}`.
- **Scenario 2 — lock denied → wait → acquire.** `POST /pattern11/lock-wait-acquired`
  starts a holder and a waiter on the same key; the waiter goes through
  `LockWaitWorkflow` (poll + exponential backoff) and acquires once the holder
  releases. Response returns `holderWorkflowId` and `waiterWorkflowId`; query both the
  lock status and `/pattern11/lock-wait-status/lock-wait-{waiterId}`.
- **Scenario 3 — lock wait timeout.** `POST /pattern11/lock-wait-timeout` holds the
  lock indefinitely so the waiter exhausts its attempts
  (`patterns.locking.wait-max-attempts`/`wait-max-timeout`, ~25s) → `LOCK_WAIT_TIMEOUT`
  escalation.

### Pattern 12 — File ordering / dependency graph · `P12-DependencyGraph` (+ `P12-OrderedProcessing`, `P12-DependencyWait`)

- **Scenario 1 — no cycles.** `POST /pattern12/no-cycles` builds the graph
  (E, A→B→C→D), validates (no cycles), topologically sorts, and processes in order →
  `DONE`, 5 files. Query `GET /pattern12/stage/{id}`.
- **Scenario 2 — missing dependencies → wait.** `POST /pattern12/dependency-wait`
  submits files whose upstream is missing; `DependencyWaitWorkflow` polls
  (`patterns.dependency.poll-interval`) until dependencies resolve, then processes
  (`WAITING_FOR_DEPENDENCIES` → `DONE`).
- **Scenario 3 — cycle detected and broken.** `POST /pattern12/cycle-resolution`
  injects an A→B→C→A cycle; it is detected, an edge is broken, then the graph is
  sorted and processed → `DONE`, 3 files.

---

## Pattern 1 — SFTP mode (real SFG poller)

Pattern 1 normally waits for a `fileArrived` signal (the `/send-signal` endpoint
simulates the external poller). With the bundled SFTP server it runs the real
flow: the poller lists the SFTP directory, performs an SFTP GET, then starts the
workflow and signals it. Enabled by default under Docker (`temporal.sftp.enabled`).

Drive it by moving a file into the polled directory, then restore it afterwards
(files are moved, never deleted):

```bash
# Linux/macOS
scripts/sftp-stage.sh payment_batch_2026.csv   # sftp_data/home -> sftp_data/incoming (poller picks it up)
scripts/sftp-restore.sh                          # sftp_data/incoming -> sftp_data/home

:: Windows
scripts\sftp-stage.cmd payment_batch_2026.csv
scripts\sftp-restore.cmd
```

The poller downloads the file and starts workflow `p01-sftp-<filename>`.

**Pattern 8** also uses real SFTP when enabled: `executeTransferSegment` performs a
byte-range SFTP GET (`SftpGateway.downloadSegment`) resuming from the checkpoint
offset, and integrity is verified against the remote file size. With SFTP disabled
it falls back to local file transfer (used by the tests).

---

## Testing

Time-skipping unit tests run all 12 patterns (plus failure branches) in-process
via Temporal's `TestWorkflowEnvironment` — no server required:

```bash
./mvnw test
```

---

## Project layout

```
src/main/java/com/example/temporalshowcase/
  config/       TemporalProperties, TemporalRuntime, TemporalConfig (client)
  worker/       ShowcaseWorker (registration; starts irrespective of Temporal)
  controller/   ShowcaseController (REST start/query/signal endpoints)
  monitor/      FileArrivalMonitor (local file watcher for Pattern 1)
  sftp/         SftpGateway, SftpFilePoller (Pattern 1)
  models/       FileMetadata, TransferRequest, etc.
  workflows/    pattern1 … pattern12
  activities/   real file / validation / transfer / lock work
src/main/resources/application.yml   all Temporal + SFTP configuration
scripts/        sftp-stage / sftp-restore (.cmd + .sh), run-project.sh, run-patterns.sh
docs/           STATUS.md, PATTERN_CURLS.md
data/           sample input files + runtime output (checkpoints/, outgoing/, quarantine/, state/)
sftp_data/      SFTP staging (home = source, incoming = polled dir)
```
