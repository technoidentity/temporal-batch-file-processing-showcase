# Pattern PowerShell scenarios

Run from the repository root. Use `curl.exe` (not PowerShell's `curl` alias). Replace placeholders with returned IDs. Add `| ConvertFrom-Json` to format JSON.

## Pattern 1 - Delayed files and SLA
Use case: wait for file arrival, signal it, monitor status, or demonstrate SLA timeout.
```powershell
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern1/signal-arrival"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern1/start-waiting?fileName=payment_batch_2026.csv"
curl.exe -s "http://localhost:8080/api/showcase/pattern1/status/<workflowId>"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern1/send-signal/<workflowId>?fileName=payment_batch_2026.csv"
New-Item -ItemType File -Force "data/incoming/payment_batch_2026.csv"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern1/timeout-test?slaSeconds=30"
```
Expected: waiting becomes `COMPLETED` after the signal; timeout becomes `SLA_VIOLATED` and fails with `SLA_TIMEOUT`.

## Pattern 2 - Large-file chunking
Use case: process large files in parallel chunks with independent retries and aggregation.
```powershell
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern2/large-file"
curl.exe -s "http://localhost:8080/api/showcase/pattern2/progress/<workflowId>"
```
Expected: `P02-LargeFileChunking` starts `P02-ChunkProcessor` children and reports chunk progress.

## Pattern 3 - Empty-file circuit breaker
Use case: skip empty files and open the breaker when the empty-file rate exceeds the threshold.
```powershell
$content = '{"fileName":"payment_batch_2026.csv","filePath":"incoming/payment_batch_2026.csv"}'
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern3/circuit-breaker" -H "Content-Type: application/json" -d $content
$empty = '{"fileName":"possibly_empty.csv","filePath":"incoming/possibly_empty.csv"}'
1..3 | ForEach-Object { curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern3/circuit-breaker" -H "Content-Type: application/json" -d $empty }
curl.exe -s "http://localhost:8080/api/showcase/pattern3/circuit-state/<workflowId>"
```
Expected: empty files are skipped; repeated empty files make state `OPEN`; later content can fail with `CIRCUIT_BREAKER_OPEN`.

## Pattern 4 - Corrupted-file saga
Use case: isolate, alert, repair, resume, or compensate when repair fails.
```powershell
$body = '{"fileName":"corrupted_claims.xml","filePath":"incoming/corrupted_claims.xml"}'
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern4/saga-recovery" -H "Content-Type: application/json" -d $body
curl.exe -s "http://localhost:8080/api/showcase/pattern4/saga-status/<workflowId>"
Move-Item "data/incoming/corrupted_claims.xml" "data/incoming/corrupted_claims.xml.bak"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern4/saga-recovery" -H "Content-Type: application/json" -d $body
Move-Item "data/incoming/corrupted_claims.xml.bak" "data/incoming/corrupted_claims.xml"
```
Expected: corrupted input is repaired; missing source exercises resend and LIFO compensation to `RESOLVED`.

## Pattern 5 - Idempotency
Use case: process the first file once and return a cached result for duplicates.
```powershell
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern5/idempotency"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern5/duplicate"
$hash = (Get-FileHash "data/incoming/daily_trades_20260831.csv" -Algorithm SHA256).Hash.ToLower()
curl.exe -s "http://localhost:8080/api/showcase/pattern5/dedup-state/<workflowId>?fileHash=$hash"
```
Expected: the duplicate is skipped and state includes `DUPLICATE_RETURNED`.

## Pattern 6 - Multi-format routing
Use case: route CSV, XML, JSON, and fixed-width files to format-specific child workflows.
```powershell
"CSV","XML","JSON","FIXED_WIDTH" | ForEach-Object { curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern6/multi-format?format=$_" }
curl.exe -s "http://localhost:8080/api/showcase/pattern6/format/<workflowId>"
```
Expected: `P06-MultiFormatRouter` starts the matching processor child.

## Pattern 7 - Permission escalation
Use case: process accessible files, auto-fix recoverable permissions, or wait for a human resolution.
```powershell
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern7/scenario1-permission-ok"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern7/scenario2-autofix"
$response = curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern7/scenario3-human-intervention" | ConvertFrom-Json
$id = $response.workflowId
curl.exe -s "http://localhost:8080/api/showcase/pattern7/escalation-status/$id"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern7/scenario4-resolve/$id?resolution=MANUALLY_FIXED_BY_ADMIN"
```
Expected: the human branch resumes after `permissionResolved`; otherwise it fails with `PERMISSION_ESCALATION_TIMEOUT`.

## Pattern 8 - Transfer recovery
Use case: complete normal transfers, resume from checkpoints, and restart after integrity failure.
```powershell
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern8/normal-transfer"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern8/resumable-transfer"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern8/integrity-failure"
curl.exe -s "http://localhost:8080/api/showcase/pattern8/progress/<workflowId>"
```
Expected: normal completes directly; resumable resumes from its offset; integrity failure restarts from byte zero.

## Pattern 9 - Adaptive rate limiting
Use case: process normally within limits and throttle when resource load is high.
```powershell
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern9/within-limits?filesToProcess=10"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern9/rate-exceeded?cycles=3"
curl.exe -s "http://localhost:8080/api/showcase/pattern9/throttle-status/<workflowId>"
```
Expected: the normal path is unthrottled; the exceeded path applies adaptive throttle tiers and waits.

## Pattern 10 - Multi-stage validation
Use case: validate BASIC, BUSINESS, ADVANCED, and FINAL stages with remediation and escalation.
```powershell
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern10/quality-ok"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern10/business-remediation"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern10/needs-remediation"
curl.exe -s "http://localhost:8080/api/showcase/pattern10/stage/<workflowId>"
```
Expected: clean data completes all gates; business errors are remediated; repeated advanced failures reach `VALIDATION_ESCALATED`.

## Pattern 11 - Distributed locking
Use case: coordinate exclusive access, wait with backoff, and escalate lock conflicts.
```powershell
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern11/lock-acquired"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern11/lock-wait-acquired"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern11/lock-wait-timeout"
curl.exe -s "http://localhost:8080/api/showcase/pattern11/lock-status/<waiterWorkflowId>"
curl.exe -s "http://localhost:8080/api/showcase/pattern11/lock-wait-status/lock-wait-<waiterWorkflowId>"
```
Expected: the waiter acquires after release in scenario 2; scenario 3 ends with `LOCK_WAIT_TIMEOUT`.

## Pattern 12 - Dependency graph ordering
Use case: topologically order files, wait for missing dependencies, and resolve cycles.
```powershell
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern12/no-cycles"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern12/dependency-wait"
curl.exe -s -X POST "http://localhost:8080/api/showcase/pattern12/cycle-resolution"
curl.exe -s "http://localhost:8080/api/showcase/pattern12/stage/<workflowId>"
```
Expected: acyclic files process in order; missing dependencies show `WAITING_FOR_DEPENDENCIES`; cycles are broken and the workflow reaches `DONE`.
