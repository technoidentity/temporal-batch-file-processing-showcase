# Pattern curl scenarios

App: `http://localhost:8080`  
Replace `<workflowId>` with the `workflowId` from the start response.  
Pretty-print with `python3 -m json.tool` (no `jq` required).

JSON bodies need `Content-Type: application/json`. Start endpoints that take no body use sample files under `data/incoming/`.

**Naming.** Workflow **types** (shown in the Temporal UI) use business names
(`Pnn-…`, e.g. `P01-FileProcessing`); workflow **IDs** are pattern-numbered
(`pNN-…`, e.g. `p01-wait-<uuid>`), including child workflows.

**Config.** All task queues, retry/timeout policies, SLAs and per-pattern knobs
are in `src/main/resources/application.yml` (`temporal.*`) — nothing hardcoded.
Timings quoted below (SLA, recovery windows, poll counts) are the defaults and
are tunable there.

**State.** Circuit-breaker (P3), idempotency (P5) and lock (P11) state is
file-backed under `data/state/*.json` and **survives an app restart**.

---

## Pattern 1 — Signal-based file arrival

> **Real SFTP mode.** With the SFTP server running and `temporal.sftp.enabled=true`
> (default under Docker), `SftpFilePoller` performs the real SFG flow: it lists the
> SFTP dir, does an SFTP GET, then starts `p01-sftp-<filename>` and signals it.
> Drive it with `scripts/sftp-stage.sh <file>` and restore with `scripts/sftp-restore.sh`.
> The endpoints below simulate that poller for a server-less demo.

### Scenario 1 — Quick demo (start + signal together)

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern1/signal-arrival \
  | python3 -m json.tool
```

### Scenario 2 — Real SFG poller flow (two separate steps)

**Step A — Start the workflow, leave it waiting**

```bash
curl -s -X POST "http://localhost:8080/api/showcase/pattern1/start-waiting?fileName=payment_batch_2026.csv" \
  | python3 -m json.tool
```

The file monitor (SFG stand-in) signals only when `data/incoming/<fileName>` is **created or touched after** this call. Existing sample files do not auto-fire. To let the monitor deliver the signal instead of Step C:

```bash
touch data/incoming/payment_batch_2026.csv
```

**Step B — Query status while it is waiting** (new terminal; paste the `workflowId` from Step A)

```bash
curl -s "http://localhost:8080/api/showcase/pattern1/status/<workflowId>" \
  | python3 -m json.tool
# → { "workflowId": "p01-wait-...", "processingStatus": "WAITING_FOR_FILE_SIGNAL" }
```

**Step C — Send the `fileArrived` signal** (simulates the SFG poller detecting the file)

```bash
curl -s -X POST "http://localhost:8080/api/showcase/pattern1/send-signal/<workflowId>?fileName=payment_batch_2026.csv" \
  | python3 -m json.tool
```

**Step D — Query again to confirm it completed**

```bash
curl -s "http://localhost:8080/api/showcase/pattern1/status/<workflowId>" \
  | python3 -m json.tool
# → { "workflowId": "p01-wait-...", "processingStatus": "COMPLETED" }
```

### Scenario 3 — SLA timeout (file never arrives)

Fires after 30 seconds — watch in Temporal UI (`http://localhost:8233`).

```bash
curl -s -X POST "http://localhost:8080/api/showcase/pattern1/timeout-test?slaSeconds=30" \
  | python3 -m json.tool
```

Query while it is counting down:

```bash
curl -s "http://localhost:8080/api/showcase/pattern1/status/<workflowId>" \
  | python3 -m json.tool
# while waiting: "WAITING_FOR_FILE_SIGNAL"
# just before fail:   "SLA_VIOLATED"
# Temporal then records FAILED with error type SLA_TIMEOUT
```

---

## Pattern 2 — Large file chunking

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern2/large-file \
  | python3 -m json.tool
```

Copy the returned `workflowId` (`p02-large-...`). The sample `large_transaction_dump.dat` is under the 100 MB single-chunk threshold, so progress is `1/1 chunks`.

```bash
curl -s "http://localhost:8080/api/showcase/pattern2/progress/<workflowId>" \
  | python3 -m json.tool
```

Example response:

```json
{
  "workflowId": "p02-large-...",
  "progress": "PROCESSING_CHUNKS — 1/1 chunks completed"
}
```

---

## Pattern 3 — Circuit breaker (empty files)

Empty vs content is decided by **bytes on disk**, not `fileSize` in the JSON. Circuit state is file-backed under `data/state/circuit-breaker.json` and survives a restart. The circuit **OPENs after 3 empty files** in the sliding window (rate > 50% with at least 3 samples). The first 1–2 empty files go `HALF_OPEN`.

### Scenario 1 — File has content (normal processing, circuit CLOSED)

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern3/circuit-breaker \
  -H "Content-Type: application/json" \
  -d '{"fileName":"payment_batch_2026.csv","filePath":"incoming/payment_batch_2026.csv"}' \
  | python3 -m json.tool
```

### Scenario 2 — Empty file (updates empty-file metrics)

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern3/circuit-breaker \
  -H "Content-Type: application/json" \
  -d '{"fileName":"possibly_empty.csv","filePath":"incoming/possibly_empty.csv"}' \
  | python3 -m json.tool
```

Run this **3 times** (no body also defaults to `possibly_empty.csv`) to trip the circuit to `OPEN`.

### Scenario 3 — Query circuit state

```bash
curl -s "http://localhost:8080/api/showcase/pattern3/circuit-state/<workflowId>" \
  | python3 -m json.tool
# → { "workflowId": "p03-cb-...", "circuitState": "CLOSED" | "HALF_OPEN" | "OPEN" }
```

### Scenario 4 — Send a file with content while circuit is OPEN (blocked)

After Scenario 2 has opened the circuit:

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern3/circuit-breaker \
  -H "Content-Type: application/json" \
  -d '{"fileName":"payment_batch_2026.csv","filePath":"incoming/payment_batch_2026.csv"}' \
  | python3 -m json.tool
```

The workflow fails with `CIRCUIT_BREAKER_OPEN` until the 5-minute recovery window elapses (then a probe is allowed in `HALF_OPEN`).

---

## Pattern 4 — Corrupted file saga

Validation uses on-disk content: `CORRUPT` / `???INVALID???` markers, or a **64-hex SHA-256** checksum that does not match. A short/non-hex `fileChecksum` is ignored.

### Scenario 1 — Corrupted file (isolate → alert → repair → resume)

Default file `corrupted_claims.xml` has corruption markers. Repair succeeds if the source file exists.

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern4/saga-recovery \
  -H "Content-Type: application/json" \
  -d '{"fileName":"corrupted_claims.xml","filePath":"incoming/corrupted_claims.xml"}' \
  | python3 -m json.tool
```

### Scenario 2 — Clean file (valid → normal processing)

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern4/saga-recovery \
  -H "Content-Type: application/json" \
  -d '{"fileName":"payment_batch_2026.csv","filePath":"incoming/payment_batch_2026.csv"}' \
  | python3 -m json.tool
```

### Scenario 3 — Query live saga status

```bash
curl -s "http://localhost:8080/api/showcase/pattern4/saga-status/<workflowId>" \
  | python3 -m json.tool
# → { "workflowId": "p04-saga-...", "sagaStatus": "VALIDATING" | "REPAIRING" | "COMPLETED" | ... }
```

### Scenario 4 — Repair failed path (source file missing)

Repair only fails when the source file is not found. Temporarily rename it, then start the saga; restore the file afterwards.

```bash
mv data/incoming/corrupted_claims.xml data/incoming/corrupted_claims.xml.bak

curl -s -X POST http://localhost:8080/api/showcase/pattern4/saga-recovery \
  -H "Content-Type: application/json" \
  -d '{"fileName":"corrupted_claims.xml","filePath":"incoming/corrupted_claims.xml"}' \
  | python3 -m json.tool

# restore
mv data/incoming/corrupted_claims.xml.bak data/incoming/corrupted_claims.xml
```

Expected path: isolate → alert → repair fails (`Source file not found`) → request resend → LIFO compensations → `RESOLVED`.

---

## Pattern 5 — Idempotent processing

Defaults already use `daily_trades_20260831.csv`. Duplicate detection is file-backed under `data/state/` and survives a restart. Run Scenario 2 after Scenario 1.

### Scenario 1 — First send (new file)

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern5/idempotency \
  | python3 -m json.tool
```

### Scenario 2 — Duplicate send (returns cached result)

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern5/duplicate \
  | python3 -m json.tool
```

### Scenario 3 — Query deduplication state

`fileHash` is required. Use the SHA-256 of the sample file (must match the hash the workflow computed):

```bash
FILE_HASH=$(sha256sum data/incoming/daily_trades_20260831.csv | awk '{print $1}')

curl -s "http://localhost:8080/api/showcase/pattern5/dedup-state/<workflowId>?fileHash=${FILE_HASH}" \
  | python3 -m json.tool
# → { "workflowId": "...", "fileHash": "...", "dedupState": "CHECKING" | "DUPLICATE_RETURNED" | ... }
```

---

## Pattern 6 — Multi-format router

### Start by format

```bash
curl -s -X POST "http://localhost:8080/api/showcase/pattern6/multi-format?format=CSV" \
  | python3 -m json.tool

curl -s -X POST "http://localhost:8080/api/showcase/pattern6/multi-format?format=XML" \
  | python3 -m json.tool

curl -s -X POST "http://localhost:8080/api/showcase/pattern6/multi-format?format=JSON" \
  | python3 -m json.tool

curl -s -X POST "http://localhost:8080/api/showcase/pattern6/multi-format?format=FIXED_WIDTH" \
  | python3 -m json.tool
```

Sample files: `records.csv`, `records.xml`, `records.json`, `large_transaction_dump.dat`.

### Query current format

```bash
curl -s "http://localhost:8080/api/showcase/pattern6/format/<workflowId>" \
  | python3 -m json.tool
# → { "workflowId": "p06-fmt-...", "currentFormat": "CSV" | "XML" | "JSON" | "FIXED_WIDTH" | ... }
```

---

## Pattern 7 — Permission escalation

### Scenario 1 — Permission already OK

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern7/scenario1-permission-ok \
  | python3 -m json.tool
```

### Scenario 2 — Auto-fix succeeds

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern7/scenario2-autofix \
  | python3 -m json.tool
```

### Scenario 3 — Human intervention (start and wait)

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern7/scenario3-human-intervention \
  | python3 -m json.tool
```

Save the `workflowId` (`p07-perm-...`). Query while it is waiting:

```bash
curl -s "http://localhost:8080/api/showcase/pattern7/escalation-status/<workflowId>" \
  | python3 -m json.tool
```

### Scenario 4 — Human resolution signal (unblocks Scenario 3)

```bash
curl -s -X POST "http://localhost:8080/api/showcase/pattern7/scenario4-resolve/<workflowId>?resolution=MANUALLY_FIXED_BY_ADMIN" \
  | python3 -m json.tool
```

### Full Scenario 3 + 4 flow

```bash
WF_ID=$(curl -s -X POST http://localhost:8080/api/showcase/pattern7/scenario3-human-intervention \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['workflowId'])")
echo "Workflow ID: $WF_ID"

curl -s "http://localhost:8080/api/showcase/pattern7/escalation-status/$WF_ID" \
  | python3 -m json.tool

curl -s -X POST "http://localhost:8080/api/showcase/pattern7/scenario4-resolve/${WF_ID}?resolution=MANUALLY_FIXED_BY_ADMIN" \
  | python3 -m json.tool
```

---

## Pattern 8 — Resumable transfer

> When `temporal.sftp.enabled=true`, each segment is a **real byte-range SFTP GET**
> (`SftpGateway.downloadSegment`) resuming from the checkpoint offset, and integrity
> is checked against the remote file size. With SFTP disabled it falls back to local
> file transfer (the mode the tests use).

### Scenario 1 — Network OK → normal transfer

Uses `NormalTransferWorkflow` (`p08-normal-...`). Completes in about 1s. **No progress query** on this path.

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern8/normal-transfer \
  | python3 -m json.tool
```

### Scenario 2 — Network failure → checkpoint → resume (`p08-transfer-...`)

First 2 connectivity checks return `DISCONNECTED` → save checkpoint → wait → resume from byte offset → validate → complete.

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern8/resumable-transfer \
  | python3 -m json.tool
```

### Scenario 3 — Integrity failure → restart from beginning (`p08-integrity-...`)

Transfer completes, first integrity check fails (simulated) → restart from byte 0 → integrity passes → complete.

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern8/integrity-failure \
  | python3 -m json.tool
```

### Live progress (Scenarios 2 and 3 only)

```bash
# Scenario 2
curl -s "http://localhost:8080/api/showcase/pattern8/progress/<p08-transfer-workflowId>" \
  | python3 -m json.tool

# Scenario 3
curl -s "http://localhost:8080/api/showcase/pattern8/progress/<p08-integrity-workflowId>" \
  | python3 -m json.tool
```

---

## Pattern 9 — Rate limiting

### Scenario 1 — Within limits (normal processing)

Workflow id prefix: `p09-normal-...`. There is **no** throttle-status query on this path.

```bash
curl -s -X POST "http://localhost:8080/api/showcase/pattern9/within-limits?filesToProcess=10" \
  | python3 -m json.tool
```

### Scenario 2 — Rate limit exceeded (adaptive throttling)

Workflow id prefix: `p09-rate-...`. Default is 3 monitoring cycles.

```bash
curl -s -X POST "http://localhost:8080/api/showcase/pattern9/rate-exceeded?cycles=3" \
  | python3 -m json.tool
```

### Live throttle status (Scenario 2 only)

```bash
curl -s "http://localhost:8080/api/showcase/pattern9/throttle-status/<p09-rate-workflowId>" \
  | python3 -m json.tool
# → { "workflowId": "p09-rate-...", "throttleStatus": "..." }
```

---

## Pattern 10 — Multi-stage validation

### Scenario 1 — All 4 stages pass (`payment_batch_2026.csv`)

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern10/quality-ok \
  | python3 -m json.tool
```

### Scenario 2 — Business validation fails → remediation → approved (`malformed_batch.csv`)

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern10/business-remediation \
  | python3 -m json.tool
```

### Scenario 3 — Advanced validation fails 3 times → escalated (`corrupted_transactions.csv`)

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern10/needs-remediation \
  | python3 -m json.tool
```

### Query current validation stage

```bash
curl -s "http://localhost:8080/api/showcase/pattern10/stage/<workflowId>" \
  | python3 -m json.tool
# → { "workflowId": "p10-...", "currentStage": "BASIC" | "BUSINESS" | "ADVANCED" | "FINAL" | ... }
```

---

## Pattern 11 — Distributed lock

Lock state is file-backed under `data/state/` and survives a restart (single-host).

### Scenario 1 — Lock acquired immediately (no contention)

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern11/lock-acquired \
  | python3 -m json.tool
```

### Scenario 2 — Lock denied → wait → eventually acquire

Returns `holderWorkflowId` and `waiterWorkflowId` (not a single `workflowId`).

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern11/lock-wait-acquired \
  | python3 -m json.tool
```

Query the waiter:

```bash
curl -s "http://localhost:8080/api/showcase/pattern11/lock-status/<waiterWorkflowId>" \
  | python3 -m json.tool

curl -s "http://localhost:8080/api/showcase/pattern11/lock-wait-status/lock-wait-<waiterWorkflowId>" \
  | python3 -m json.tool
```

### Scenario 3 — Lock wait times out and escalates

The waiter child polls **4 times** (25s cap) then fails with `LOCK_WAIT_TIMEOUT`.

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern11/lock-wait-timeout \
  | python3 -m json.tool
```

```bash
curl -s "http://localhost:8080/api/showcase/pattern11/lock-status/<waiterWorkflowId>" \
  | python3 -m json.tool

curl -s "http://localhost:8080/api/showcase/pattern11/lock-wait-status/lock-wait-<waiterWorkflowId>" \
  | python3 -m json.tool
```

---

## Pattern 12 — Dependency graph

### Scenario 1 — No cycles (E, A→B→C→D)

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern12/no-cycles \
  | python3 -m json.tool

curl -s "http://localhost:8080/api/showcase/pattern12/stage/<workflowId>" \
  | python3 -m json.tool
# expect: "stage": "DONE", "completedFiles": "5"
```

### Scenario 2 — Missing deps → wait workflow (~6s of polling)

2 polls × 3s, then simulated arrival.

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern12/dependency-wait \
  | python3 -m json.tool

curl -s "http://localhost:8080/api/showcase/pattern12/stage/<workflowId>" \
  | python3 -m json.tool
# while waiting: "WAITING_FOR_DEPENDENCIES"  then  "DONE"
```

### Scenario 3 — Cycle detected and broken

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern12/cycle-resolution \
  | python3 -m json.tool

curl -s "http://localhost:8080/api/showcase/pattern12/stage/<workflowId>" \
  | python3 -m json.tool
# expect: stage passes CYCLE_RESOLUTION then DONE, "completedFiles": "3"
```

---

## Quick one-liner (any start)

```bash
curl -s -X POST http://localhost:8080/api/showcase/pattern1/signal-arrival \
  | python3 -m json.tool
```
