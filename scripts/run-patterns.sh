#!/usr/bin/env bash
# Interactive / CLI runner for all 12 showcase patterns and their curl scenarios.
# Local helper — listed in .gitignore; not part of the committed project.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BASE_URL="${BASE_URL:-http://localhost:8080}"
API="$BASE_URL/api/showcase"

if [ -t 1 ]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'
  CYAN=$'\033[36m'; RED=$'\033[31m'; RESET=$'\033[0m'
else
  BOLD=""; DIM=""; GREEN=""; YELLOW=""; CYAN=""; RED=""; RESET=""
fi

PASS=0
FAIL=0

header() { echo; echo "${BOLD}${CYAN}== $* ==${RESET}"; }
ok()     { echo "${GREEN}OK${RESET}  $*"; PASS=$((PASS + 1)); }
warn()   { echo "${YELLOW}WARN${RESET} $*"; }
fail()   { echo "${RED}FAIL${RESET} $*"; FAIL=$((FAIL + 1)); }

pretty() {
  python3 -m json.tool 2>/dev/null || cat
}

extract() {
  local key="$1"
  python3 -c "import sys,json
try:
    d=json.load(sys.stdin)
    v=d.get('$key','')
    print(v if v is not None else '')
except Exception:
    print('')"
}

# POST / GET helpers. Last JSON body is stored in LAST_BODY.
LAST_BODY=""
LAST_CODE=""

post() {
  local path="$1"; shift || true
  local url="$API$path"
  echo "${DIM}POST $url${RESET}"
  LAST_BODY="$(curl -sS -w $'\n%{http_code}' -X POST "$url" "$@" || true)"
  LAST_CODE="${LAST_BODY##*$'\n'}"
  LAST_BODY="${LAST_BODY%$'\n'*}"
  echo "$LAST_BODY" | pretty
  if [[ "$LAST_CODE" =~ ^2 ]]; then
    ok "HTTP $LAST_CODE"
  else
    fail "HTTP ${LAST_CODE:-curl-error}  POST $path"
  fi
}

get() {
  local path="$1"
  local url="$API$path"
  echo "${DIM}GET  $url${RESET}"
  LAST_BODY="$(curl -sS -w $'\n%{http_code}' "$url" || true)"
  LAST_CODE="${LAST_BODY##*$'\n'}"
  LAST_BODY="${LAST_BODY%$'\n'*}"
  echo "$LAST_BODY" | pretty
  if [[ "$LAST_CODE" =~ ^2 ]]; then
    ok "HTTP $LAST_CODE"
  else
    fail "HTTP ${LAST_CODE:-curl-error}  GET $path"
  fi
}

wf() { echo "$LAST_BODY" | extract workflowId; }

wait_health() {
  if curl -fsS "$BASE_URL/actuator/health" >/dev/null 2>&1; then
    return 0
  fi
  echo "App is not up at $BASE_URL"
  if [ -x "$ROOT/scripts/run-project.sh" ]; then
    read -r -p "Start the Docker stack now? [Y/n] " ans || true
    ans="${ans:-Y}"
    if [[ "$ans" =~ ^[Yy]$ ]]; then
      "$ROOT/scripts/run-project.sh" up
      return 0
    fi
  fi
  echo "Start it with:  scripts/run-project.sh" >&2
  return 1
}

# ── Pattern implementations ──────────────────────────────────────────────────

run_p1() {
  header "Pattern 1 — Delayed files / signal + SLA"
  local id

  echo "${BOLD}Scenario 1${RESET} — start + signal together"
  post /pattern1/signal-arrival
  id="$(wf)"
  [ -n "$id" ] && sleep 1 && get "/pattern1/status/$id"

  echo "${BOLD}Scenario 2${RESET} — start-waiting, then send-signal"
  post "/pattern1/start-waiting?fileName=payment_batch_2026.csv"
  id="$(wf)"
  if [ -n "$id" ]; then
    sleep 1
    get "/pattern1/status/$id"
    post "/pattern1/send-signal/${id}?fileName=payment_batch_2026.csv"
    sleep 1
    get "/pattern1/status/$id"
  fi

  echo "${BOLD}Scenario 3${RESET} — file monitor (touch incoming file)"
  post "/pattern1/start-waiting?fileName=payment_batch_2026.csv"
  id="$(wf)"
  if [ -n "$id" ]; then
    sleep 1
    get "/pattern1/status/$id"
    touch "$ROOT/data/incoming/payment_batch_2026.csv"
    echo "${DIM}touched data/incoming/payment_batch_2026.csv — waiting for poller (~3s)${RESET}"
    sleep 4
    get "/pattern1/status/$id"
  fi

  echo "${BOLD}Scenario 4${RESET} — SLA timeout (8s, file never arrives)"
  post "/pattern1/timeout-test?slaSeconds=8"
  id="$(wf)"
  if [ -n "$id" ]; then
    sleep 1
    get "/pattern1/status/$id"
    echo "${DIM}waiting for SLA timer...${RESET}"
    sleep 9
    get "/pattern1/status/$id" || true
  fi

  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx 'batch-sftp'; then
    echo "${BOLD}Scenario 5${RESET} — SFTP poller (stage → GET → fileArrived)"
    if [ -f "$ROOT/sftp_data/home/payment_batch_2026.csv" ]; then
      "$ROOT/scripts/sftp-stage.sh" payment_batch_2026.csv || true
      echo "${DIM}waiting up to 15s for SFTP poller...${RESET}"
      local i
      for i in 1 2 3 4 5; do
        sleep 3
        if curl -fsS "$API/pattern1/status/p01-sftp-payment_batch_2026.csv" >/dev/null 2>&1; then
          get "/pattern1/status/p01-sftp-payment_batch_2026.csv"
          break
        fi
      done
      "$ROOT/scripts/sftp-restore.sh" || true
    else
      warn "sftp_data/home/payment_batch_2026.csv not present — skip SFTP stage"
    fi
  else
    warn "SFTP container not running — skip Pattern 1 SFTP scenario"
  fi
}

run_p2() {
  header "Pattern 2 — Large file / chunking"
  post /pattern2/large-file
  local id; id="$(wf)"
  [ -n "$id" ] && sleep 2 && get "/pattern2/progress/$id"
}

run_p3() {
  header "Pattern 3 — Empty files / circuit breaker"
  local id

  echo "${BOLD}Scenario 1${RESET} — file has content (CLOSED)"
  post /pattern3/circuit-breaker \
    -H "Content-Type: application/json" \
    -d '{"fileName":"payment_batch_2026.csv","filePath":"incoming/payment_batch_2026.csv"}'
  id="$(wf)"
  [ -n "$id" ] && sleep 1 && get "/pattern3/circuit-state/$id"

  echo "${BOLD}Scenario 2${RESET} — empty file × 3 (trip OPEN)"
  local n
  for n in 1 2 3; do
    echo "${DIM}empty file run $n/3${RESET}"
    post /pattern3/circuit-breaker \
      -H "Content-Type: application/json" \
      -d '{"fileName":"possibly_empty.csv","filePath":"incoming/possibly_empty.csv"}'
    id="$(wf)"
    [ -n "$id" ] && sleep 1 && get "/pattern3/circuit-state/$id"
  done

  echo "${BOLD}Scenario 3${RESET} — content file while circuit may be OPEN (blocked)"
  post /pattern3/circuit-breaker \
    -H "Content-Type: application/json" \
    -d '{"fileName":"payment_batch_2026.csv","filePath":"incoming/payment_batch_2026.csv"}'
  id="$(wf)"
  [ -n "$id" ] && sleep 1 && get "/pattern3/circuit-state/$id" || true
}

run_p4() {
  header "Pattern 4 — Corrupted file / saga"
  local id src bak
  src="$ROOT/data/incoming/corrupted_claims.xml"
  bak="$ROOT/data/incoming/corrupted_claims.xml.bak"

  echo "${BOLD}Scenario 1${RESET} — corrupted file (isolate → repair)"
  post /pattern4/saga-recovery \
    -H "Content-Type: application/json" \
    -d '{"fileName":"corrupted_claims.xml","filePath":"incoming/corrupted_claims.xml"}'
  id="$(wf)"
  [ -n "$id" ] && sleep 2 && get "/pattern4/saga-status/$id"

  echo "${BOLD}Scenario 2${RESET} — clean file (normal processing)"
  post /pattern4/saga-recovery \
    -H "Content-Type: application/json" \
    -d '{"fileName":"payment_batch_2026.csv","filePath":"incoming/payment_batch_2026.csv"}'
  id="$(wf)"
  [ -n "$id" ] && sleep 1 && get "/pattern4/saga-status/$id"

  echo "${BOLD}Scenario 3${RESET} — repair fails (source missing) then restore"
  if [ -f "$src" ]; then
    mv "$src" "$bak"
    trap 'mv -f "$bak" "$src" 2>/dev/null || true' RETURN
    post /pattern4/saga-recovery \
      -H "Content-Type: application/json" \
      -d '{"fileName":"corrupted_claims.xml","filePath":"incoming/corrupted_claims.xml"}'
    id="$(wf)"
    [ -n "$id" ] && sleep 2 && get "/pattern4/saga-status/$id"
    mv -f "$bak" "$src"
    trap - RETURN
  else
    warn "corrupted_claims.xml missing — skip repair-fail scenario"
  fi
}

run_p5() {
  header "Pattern 5 — Duplicate files / idempotency"
  echo "${BOLD}Scenario 1${RESET} — first send"
  post /pattern5/idempotency
  local id; id="$(wf)"
  echo "${BOLD}Scenario 2${RESET} — duplicate send (cached)"
  post /pattern5/duplicate
  if [ -n "$id" ] && [ -f "$ROOT/data/incoming/daily_trades_20260831.csv" ]; then
    local hash
    hash="$(sha256sum "$ROOT/data/incoming/daily_trades_20260831.csv" | awk '{print $1}')"
    echo "${BOLD}Scenario 3${RESET} — query dedup state"
    sleep 1
    get "/pattern5/dedup-state/${id}?fileHash=${hash}"
  fi
}

run_p6() {
  header "Pattern 6 — Format mismatch / router"
  local fmt id
  for fmt in CSV XML JSON FIXED_WIDTH; do
    echo "${BOLD}Scenario ($fmt)${RESET}"
    post "/pattern6/multi-format?format=$fmt"
    id="$(wf)"
    [ -n "$id" ] && sleep 1 && get "/pattern6/format/$id"
  done
}

run_p7() {
  header "Pattern 7 — Permission / escalation"
  local id

  echo "${BOLD}Scenario 1${RESET} — permission already OK"
  post /pattern7/scenario1-permission-ok
  id="$(wf)"
  [ -n "$id" ] && sleep 1 && get "/pattern7/escalation-status/$id"

  echo "${BOLD}Scenario 2${RESET} — auto-fix succeeds"
  post /pattern7/scenario2-autofix
  id="$(wf)"
  [ -n "$id" ] && sleep 1 && get "/pattern7/escalation-status/$id"

  echo "${BOLD}Scenario 3+4${RESET} — human intervention then resolve"
  post /pattern7/scenario3-human-intervention
  id="$(wf)"
  if [ -n "$id" ]; then
    sleep 1
    get "/pattern7/escalation-status/$id"
    post "/pattern7/scenario4-resolve/${id}?resolution=MANUALLY_FIXED_BY_ADMIN"
    sleep 1
    get "/pattern7/escalation-status/$id"
  fi
}

run_p8() {
  header "Pattern 8 — Network / resumable transfer"
  local id

  echo "${BOLD}Scenario 1${RESET} — normal transfer (network OK)"
  post /pattern8/normal-transfer
  id="$(wf)"

  echo "${BOLD}Scenario 2${RESET} — network failure → checkpoint → resume"
  post /pattern8/resumable-transfer
  id="$(wf)"
  if [ -n "$id" ]; then
    sleep 2
    get "/pattern8/progress/$id"
    sleep 4
    get "/pattern8/progress/$id"
  fi

  echo "${BOLD}Scenario 3${RESET} — integrity failure → restart from beginning"
  post /pattern8/integrity-failure
  id="$(wf)"
  if [ -n "$id" ]; then
    sleep 2
    get "/pattern8/progress/$id"
    sleep 4
    get "/pattern8/progress/$id"
  fi
}

run_p9() {
  header "Pattern 9 — Resource exhaustion / rate limiting"
  echo "${BOLD}Scenario 1${RESET} — within limits"
  post "/pattern9/within-limits?filesToProcess=10"
  echo "${BOLD}Scenario 2${RESET} — rate exceeded (3 cycles)"
  post "/pattern9/rate-exceeded?cycles=3"
  local id; id="$(wf)"
  if [ -n "$id" ]; then
    sleep 2
    get "/pattern9/throttle-status/$id"
  fi
}

run_p10() {
  header "Pattern 10 — Data quality / multi-stage"
  local id
  echo "${BOLD}Scenario 1${RESET} — all stages pass"
  post /pattern10/quality-ok
  id="$(wf)"; [ -n "$id" ] && sleep 1 && get "/pattern10/stage/$id"
  echo "${BOLD}Scenario 2${RESET} — business remediation"
  post /pattern10/business-remediation
  id="$(wf)"; [ -n "$id" ] && sleep 1 && get "/pattern10/stage/$id"
  echo "${BOLD}Scenario 3${RESET} — advanced fails → escalate"
  post /pattern10/needs-remediation
  id="$(wf)"; [ -n "$id" ] && sleep 1 && get "/pattern10/stage/$id"
}

run_p11() {
  header "Pattern 11 — Concurrent access / locking"
  local holder waiter

  echo "${BOLD}Scenario 1${RESET} — lock acquired (no contention)"
  post /pattern11/lock-acquired
  local id; id="$(wf)"
  [ -n "$id" ] && sleep 1 && get "/pattern11/lock-status/$id"

  echo "${BOLD}Scenario 2${RESET} — lock denied → wait → acquire"
  post /pattern11/lock-wait-acquired
  holder="$(echo "$LAST_BODY" | extract holderWorkflowId)"
  waiter="$(echo "$LAST_BODY" | extract waiterWorkflowId)"
  if [ -n "$waiter" ]; then
    sleep 2
    get "/pattern11/lock-status/$waiter"
    get "/pattern11/lock-wait-status/lock-wait-$waiter"
    sleep 8
    get "/pattern11/lock-status/$waiter"
  fi

  echo "${BOLD}Scenario 3${RESET} — lock wait timeout (~25s)"
  post /pattern11/lock-wait-timeout
  waiter="$(echo "$LAST_BODY" | extract waiterWorkflowId)"
  if [ -n "$waiter" ]; then
    sleep 2
    get "/pattern11/lock-status/$waiter"
    echo "${DIM}waiting for LOCK_WAIT_TIMEOUT...${RESET}"
    sleep 26
    get "/pattern11/lock-status/$waiter" || true
  fi
}

run_p12() {
  header "Pattern 12 — File ordering / dependency graph"
  local id
  echo "${BOLD}Scenario 1${RESET} — no cycles"
  post /pattern12/no-cycles
  id="$(wf)"; [ -n "$id" ] && sleep 2 && get "/pattern12/stage/$id"
  echo "${BOLD}Scenario 2${RESET} — missing deps → wait (~6s)"
  post /pattern12/dependency-wait
  id="$(wf)"
  if [ -n "$id" ]; then
    sleep 2
    get "/pattern12/stage/$id"
    sleep 6
    get "/pattern12/stage/$id"
  fi
  echo "${BOLD}Scenario 3${RESET} — cycle detected and broken"
  post /pattern12/cycle-resolution
  id="$(wf)"; [ -n "$id" ] && sleep 2 && get "/pattern12/stage/$id"
}

declare -A PATTERN_NAME=(
  [1]="Delayed files / signal + SLA"
  [2]="Large files / chunking"
  [3]="Empty files / circuit breaker"
  [4]="Corrupted files / saga"
  [5]="Duplicate files / idempotency"
  [6]="Format mismatches / router"
  [7]="Permission / escalation"
  [8]="Network / resumable transfer"
  [9]="Resource exhaustion / rate limiting"
  [10]="Data quality / multi-stage"
  [11]="Concurrent access / locking"
  [12]="File ordering / dependency graph"
)

run_pattern() {
  case "$1" in
    1)  run_p1 ;;
    2)  run_p2 ;;
    3)  run_p3 ;;
    4)  run_p4 ;;
    5)  run_p5 ;;
    6)  run_p6 ;;
    7)  run_p7 ;;
    8)  run_p8 ;;
    9)  run_p9 ;;
    10) run_p10 ;;
    11) run_p11 ;;
    12) run_p12 ;;
    *) echo "Unknown pattern: $1" >&2; return 1 ;;
  esac
}

print_menu() {
  echo
  echo "${BOLD}Batch File Processing — pick pattern(s) to run${RESET}"
  echo "${DIM}App: $BASE_URL   Temporal UI: http://localhost:8233${RESET}"
  echo
  echo "  ${BOLD}0)${RESET}  All 12 patterns (every scenario)"
  local n
  for n in $(seq 1 12); do
    printf "  ${BOLD}%2s)${RESET}  P%-2s %s\n" "$n" "$n" "${PATTERN_NAME[$n]}"
  done
  echo
  echo "${DIM}Type one number, several numbers, ranges, or all.${RESET}"
  echo "${DIM}Examples:  3     |  1 8 11     |  1,3,8-10     |  all${RESET}"
}

# Parse "all" / "0" / "1 3 8" / "1,3,8-10" into unique sorted pattern numbers.
parse_selection() {
  local raw="$*"
  raw="${raw//,/ }"
  raw="$(echo "$raw" | tr '[:upper:]' '[:lower:]')"
  if [ -z "$raw" ]; then
    return 1
  fi
  if [[ "$raw" == "all" || "$raw" == "0" || "$raw" == "a" ]]; then
    seq 1 12
    return 0
  fi
  local tok a b n
  local -a out=()
  for tok in $raw; do
    if [[ "$tok" =~ ^([0-9]+)-([0-9]+)$ ]]; then
      a="${BASH_REMATCH[1]}"; b="${BASH_REMATCH[2]}"
      if [ "$a" -gt "$b" ]; then n="$a"; a="$b"; b="$n"; fi
      for n in $(seq "$a" "$b"); do out+=("$n"); done
    elif [[ "$tok" =~ ^[0-9]+$ ]]; then
      out+=("$tok")
    else
      echo "Cannot parse: $tok" >&2
      return 1
    fi
  done
  local -A seen=()
  local -a uniq=()
  for n in "${out[@]}"; do
    if [ "$n" -eq 0 ]; then seq 1 12; return 0; fi
    if [ "$n" -lt 1 ] || [ "$n" -gt 12 ]; then
      echo "Pattern $n is out of range (1-12)" >&2
      return 1
    fi
    if [ -z "${seen[$n]:-}" ]; then
      seen[$n]=1
      uniq+=("$n")
    fi
  done
  printf '%s\n' "${uniq[@]}" | sort -n
}

usage() {
  cat <<EOF
Usage:
  scripts/run-patterns.sh                 interactive menu
  scripts/run-patterns.sh all             every pattern + every scenario
  scripts/run-patterns.sh 1 8 11          those patterns
  scripts/run-patterns.sh 1,3,8-10        commas and ranges also work

Requires a running app (scripts/run-project.sh).
EOF
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  wait_health || exit 1

  local -a selected=()
  if [ "$#" -gt 0 ]; then
    mapfile -t selected < <(parse_selection "$@")
  elif [ -t 0 ]; then
    print_menu
    echo
    read -r -p "${BOLD}Select pattern(s): ${RESET}" answer
    mapfile -t selected < <(parse_selection "$answer")
  else
    echo "No TTY and no pattern arguments. Use: scripts/run-patterns.sh all" >&2
    usage
    exit 1
  fi

  if [ "${#selected[@]}" -eq 0 ]; then
    echo "Nothing selected." >&2
    exit 1
  fi

  echo
  echo "${BOLD}Will run:${RESET}"
  local n
  for n in "${selected[@]}"; do
    echo "  P$n  ${PATTERN_NAME[$n]}"
  done
  echo

  if [ -t 0 ]; then
    read -r -p "Continue? [Y/n] " go || true
    go="${go:-Y}"
    [[ "$go" =~ ^[Yy]$ ]] || { echo "Cancelled."; exit 0; }
  fi

  for n in "${selected[@]}"; do
    run_pattern "$n"
  done

  echo
  echo "${BOLD}Done.${RESET}  ${GREEN}$PASS ok${RESET}  ${RED}$FAIL failed HTTP calls${RESET}"
  echo "Inspect executions: ${CYAN}http://localhost:8233${RESET}"
  if [ "$FAIL" -gt 0 ]; then
    exit 1
  fi
}

main "$@"
