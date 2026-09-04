# PDF use-case implementation matrix

The repository implements all 12 operational patterns described in the PDF as executable Temporal demonstrations. The controller exposes a start endpoint for each pattern, `ShowcaseWorker` registers the workflow implementations, and `PatternsWorkflowTest` exercises the patterns in-process.

This is a reference/demo implementation. In particular, lock, idempotency, and circuit-breaker state are persisted under the local `data/state/` directory; some failure conditions are deterministic simulations selected by the scenario endpoints; and the SFTP integration is enabled/configured separately.

## At-a-glance coverage

| PDF pattern | Operational use cases demonstrated | Primary workflow(s) | Coverage |
|---|---|---|---|
| 1. Delayed files | Signal arrival, local watcher, SFTP arrival, SLA timeout | `FileProcessingWorkflow` | Complete demo coverage |
| 2. Large files | Chunk sizing, parallel child processing, aggregation/retry | `LargeFileProcessingWorkflow`, `ProcessChunkWorkflow` | Complete demo coverage |
| 3. Empty files | Empty-file fast path, sliding-window breaker, open/half-open/closed behavior | `CircuitBreakerWorkflow` | Complete demo coverage |
| 4. Corrupted files | Quarantine/alert/repair, clean path, failed repair and compensation | `CorruptedFileSagaWorkflow` | Complete demo coverage |
| 5. Duplicate files | First claim, duplicate detection, cached-result return | `IdempotentFileProcessingWorkflow` | Complete demo coverage |
| 6. Format mismatches | Detection and routing for CSV, XML, JSON, fixed-width | `MultiFormatHandlerWorkflow` plus format children | Complete demo coverage |
| 7. Permission issues | Permission OK, automatic fix, human escalation and resolution | `PermissionEscalationWorkflow` | Complete demo coverage |
| 8. Network failures | Normal transfer, checkpoint/resume, integrity-failure restart | `NormalTransferWorkflow`, `ResumableTransferWorkflow` | Complete demo coverage |
| 9. Resource exhaustion | Normal processing and adaptive throttling under load | `NormalProcessingWorkflow`, `RateLimitingWorkflow` | Complete demo coverage |
| 10. Data quality | Four validation gates, remediation loop, manual escalation | `MultiStageValidationWorkflow` | Complete demo coverage |
| 11. Concurrent access | Immediate lock, wait/backoff/acquire, wait timeout/escalation | `DistributedLockingWorkflow`, `LockWaitWorkflow` | Complete demo coverage |
| 12. Ordering dependencies | Acyclic order, missing-dependency wait, cycle resolution | `DependencyGraphWorkflow`, `OrderedProcessingWorkflow`, `DependencyWaitWorkflow` | Complete demo coverage |

## Pattern details

### 1. Delayed files - signal-based detection

**Use cases covered.** A workflow can wait for a `fileArrived` signal within a configurable SLA; it can be signaled through REST, the local file-arrival monitor, or the SFTP poller; and it fails with an SLA timeout when the file never arrives.

**Implementation.** `workflows/pattern1/FileProcessingWorkflowImpl` waits for the signal, exposes a processing-status query, and then invokes file processing with configured retry/timeout policy. `monitor/FileArrivalMonitor` watches `data/incoming/`; `sftp/SftpFilePoller` lists/downloads SFTP files and starts/signals the workflow.

**How demonstrated.** `POST /pattern1/signal-arrival` starts and immediately signals; `/start-waiting` starts a waiting execution; `/send-signal/{id}` supplies the signal; `/timeout-test` demonstrates the timeout. Creating a file in `data/incoming/` demonstrates the local watcher, while the SFTP staging scripts demonstrate the real poller path.

**Workflows started.** `P01-FileProcessing` (REST/local monitor/SFTP); the SFTP path uses IDs such as `p01-sftp-*`.

### 2. Large files - chunked processing

**Use cases covered.** Files are sized and split according to configured limits, chunks run in parallel, individual chunk work retries independently, and results are aggregated and validated.

**Implementation.** `LargeFileProcessingWorkflowImpl` obtains file statistics, calculates chunk metadata, starts one `ProcessChunkWorkflow` child per chunk, waits for all promises, and invokes aggregation. `ProcessChunkWorkflowImpl` downloads and processes one chunk with separate retry/timeout policies.

**How demonstrated.** `POST /pattern2/large-file`; `GET /pattern2/progress/{id}`. The bundled sample is below the configured threshold, so it normally shows a valid one-chunk run; larger input exercises multiple children.

**Workflows started.** Parent `P02-LargeFileChunking`; child `P02-ChunkProcessor` executions on the chunk queue.

### 3. Empty files - circuit breaker

**Use cases covered.** Empty files are validated and skipped; recent empty-file outcomes are tracked; the breaker opens above the configured failure-rate threshold, permits a half-open recovery probe after the recovery timeout, and returns to closed on success.

**Implementation.** `CircuitBreakerWorkflowImpl` reads breaker state, validates file content before processing, records empty/success/failure outcomes, blocks work while open, and applies recovery logic. `CircuitBreakerActivitiesImpl` persists the state under `data/state/`.

**How demonstrated.** `POST /pattern3/circuit-breaker` with a content file demonstrates normal processing; repeated empty-file submissions trip the breaker; a content file while open demonstrates fast failure. `GET /pattern3/circuit-state/{id}` queries state.

**Workflows started.** `P03-EmptyFileCircuitBreaker`.

### 4. Corrupted files - saga recovery

**Use cases covered.** Corruption is isolated and alerted, repair is attempted, clean files use the normal path, and failed repair triggers resend/escalation plus reverse-order compensations.

**Implementation.** `CorruptedFileSagaWorkflowImpl` coordinates validation, quarantine, alerting, repair, and resume. `SagaActivitiesImpl` performs the side effects and compensation operations; compensation records are executed in LIFO order.

**How demonstrated.** `POST /pattern4/saga-recovery` with a corrupted sample demonstrates repair; a clean sample bypasses the saga; removing the source before repair demonstrates the failed-repair compensation branch. `GET /pattern4/saga-status/{id}` queries progress.

**Workflows started.** `P04-CorruptedFileSaga`.

### 5. Duplicate files - idempotency

**Use cases covered.** The first file claims an idempotency key and processes; a repeated file hash is recognized as a duplicate; the prior result is returned without reprocessing.

**Implementation.** `IdempotentFileProcessingWorkflowImpl` hashes the file, atomically claims/checks the key, waits when another execution is in progress, and returns the stored result for duplicates. `IdempotencyActivitiesImpl` persists the deduplication record under `data/state/`.

**How demonstrated.** `POST /pattern5/idempotency` runs the first submission; `/duplicate` repeats it; `GET /pattern5/dedup-state/{id}?fileHash=...` exposes the dedup state.

**Workflows started.** `P05-DuplicateFileIdempotency`.

### 6. Format mismatches - multi-format handler

**Use cases covered.** The incoming format is detected and routed to the correct CSV, XML, JSON, or fixed-width processor.

**Implementation.** `MultiFormatHandlerWorkflowImpl` detects/validates format and starts the matching child workflow: `CsvProcessorWorkflow`, `XmlProcessorWorkflow`, `JsonProcessorWorkflow`, or `FixedWidthProcessorWorkflow`. Format-specific activities perform parsing and normalization.

**How demonstrated.** `POST /pattern6/multi-format?format=CSV|XML|JSON|FIXED_WIDTH`; sample files include `records.csv`, `records.xml`, `records.json`, and `large_transaction_dump.dat`. `GET /pattern6/format/{id}` queries the route/result.

**Workflows started.** Parent `P06-MultiFormatRouter` plus one `P06-*Processor` child for the selected format.

### 7. Permission issues - escalation

**Use cases covered.** Accessible files process normally; recoverable permission failures are auto-fixed; unfixable failures wait for a human resolution signal and time out if unresolved.

**Implementation.** `PermissionEscalationWorkflowImpl` checks permissions, selects an auto-fix strategy, waits for `permissionResolved` when needed, and enforces the configured human-resolution timeout. `PermissionActivitiesImpl` models permission checks and remediation.

**How demonstrated.** `/scenario1-permission-ok`, `/scenario2-autofix`, and `/scenario3-human-intervention` select the three branches; `/scenario4-resolve/{id}` sends the human-resolution signal. `GET /pattern7/escalation-status/{id}` queries status.

**Workflows started.** `P07-PermissionEscalation`.

### 8. Network failures - resumable transfer

**Use cases covered.** A healthy transfer completes in one pass; network failure saves a checkpoint and resumes from the byte offset; an integrity failure restarts from byte zero and revalidates.

**Implementation.** `NormalTransferWorkflowImpl` handles single-shot transfer. `ResumableTransferWorkflowImpl` runs segmented transfer, records `CheckpointData`, polls for network recovery, resumes from the checkpoint, and verifies integrity. `SftpGateway` uses byte-range SFTP GET when SFTP mode is enabled; local transfer is used otherwise.

**How demonstrated.** `/normal-transfer` is the healthy path; `/resumable-transfer` simulates initial connectivity failures and recovery; `/integrity-failure` simulates a failed first integrity check and restart. `GET /pattern8/progress/{id}` queries progress.

**Workflows started.** `P08-NormalTransfer` or `P08-ResumableTransfer`.

### 9. Resource exhaustion - rate limiting

**Use cases covered.** Work proceeds without throttling under normal load; CPU/disk/thread measurements drive adaptive throttle tiers and waiting under high load.

**Implementation.** `RateLimitingWorkflowImpl` periodically reads resource metrics, computes a weighted load, selects a throttle rate, and waits/rechecks within configured cycle limits. `ResourceMonitorActivitiesImpl` supplies the metrics. `NormalProcessingWorkflowImpl` is the control path.

**How demonstrated.** `/within-limits?filesToProcess=10` runs the normal path; `/rate-exceeded?cycles=3` runs adaptive throttling. `GET /pattern9/throttle-status/{id}` queries state.

**Workflows started.** `P09-NormalProcessing` for the control path; `P09-AdaptiveRateLimiting` for the throttled path.

### 10. Data quality - multi-stage validation

**Use cases covered.** BASIC, BUSINESS, ADVANCED, and FINAL validation gates run in sequence; business failures can be remediated and revalidated; repeated advanced failures escalate for manual review.

**Implementation.** `MultiStageValidationWorkflowImpl` maintains a validation context and stage, runs each gate, chooses a stage-specific remediation strategy, loops up to the configured remediation limit, and invokes escalation when remediation fails. `ValidationActivitiesImpl` implements the checks/remediation.

**How demonstrated.** `/quality-ok` passes all four gates; `/business-remediation` triggers `correctMissingData` and revalidation; `/needs-remediation` uses corruption markers to reach the escalation branch. `GET /pattern10/stage/{id}` queries the current stage.

**Workflows started.** `P10-DataQualityValidation`.

### 11. Concurrent access - distributed locking

**Use cases covered.** A file lock is acquired before exclusive processing and released afterward; a denied waiter polls with backoff until the lock is released; exhausted waiting escalates.

**Implementation.** `DistributedLockingWorkflowImpl` derives a file/checksum lock key, acquires it, processes through a child `FileProcessingWorkflow`, and releases it in success and failure paths. `LockWaitWorkflowImpl` performs bounded polling/backoff. `LockActivitiesImpl` owns the local lock registry and dead-owner cleanup.

**How demonstrated.** `/lock-acquired` demonstrates no contention; `/lock-wait-acquired` starts a holder and waiter on the same key; `/lock-wait-timeout` keeps the holder active so the waiter escalates. Lock and waiter status endpoints expose both executions.

**Workflows started.** `P11-DistributedFileLock`; on contention, a `P11-LockWait` child plus the processing child `P01-FileProcessing`.

### 12. File ordering - dependency graph

**Use cases covered.** Dependencies are extracted into a graph, cycles are detected/resolved, topological ordering is calculated, missing dependencies cause durable waiting, and files process only after prerequisites complete.

**Implementation.** `DependencyGraphWorkflowImpl` builds/validates the graph, invokes topological sort and cycle resolution activities, checks dependency status, and starts ordered child processing. `DependencyWaitWorkflowImpl` polls for missing prerequisites; `OrderedProcessingWorkflowImpl` represents the ordered pipeline.

**How demonstrated.** `/no-cycles` processes the `E, A -> B -> C -> D` graph; `/dependency-wait` submits missing upstream dependencies and waits; `/cycle-resolution` injects `A -> B -> C -> A`, breaks the cycle, sorts, and processes. `GET /pattern12/stage/{id}` queries stage.

**Workflows started.** `P12-DependencyGraph`; as needed, `P12-DependencyWait`, `P12-OrderedProcessing`, and `P01-FileProcessing` children.

## Verification sources in the repository

- `src/main/java/com/example/temporalshowcase/controller/ShowcaseController.java` - start, signal, and query endpoints for all patterns.
- `src/main/java/com/example/temporalshowcase/worker/ShowcaseWorker.java` - workflow/activity registration and task queues.
- `src/main/java/com/example/temporalshowcase/workflows/pattern1` through `pattern12` - workflow implementations.
- `src/test/java/com/example/temporalshowcase/PatternsWorkflowTest.java` - time-skipping workflow coverage.
- `docs/PATTERN_CURLS.md` and `README.md` - runnable scenario catalog.

