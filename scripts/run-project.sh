#!/usr/bin/env bash
# Start / stop the Temporal + app + SFTP Docker stack and wait until the API is healthy.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BASE_URL="${BASE_URL:-http://localhost:8080}"
COMPOSE=(docker compose)

usage() {
  cat <<EOF
Usage: scripts/run-project.sh [up|down|status|logs]

  up       build and start Temporal, app, SFTP; wait for health (default)
  down     stop containers
  status   show compose status + health
  logs     tail the app container
EOF
}

wait_healthy() {
  echo -n "Waiting for $BASE_URL/actuator/health"
  local i
  for i in $(seq 1 90); do
    if curl -fsS "$BASE_URL/actuator/health" >/dev/null 2>&1; then
      echo " — up."
      return 0
    fi
    echo -n "."
    sleep 2
  done
  echo
  echo "Timed out waiting for the app. Check: docker compose logs app" >&2
  return 1
}

cmd="${1:-up}"
case "$cmd" in
  -h|--help|help) usage; exit 0 ;;
  up|start)
    echo "Starting Temporal + app + SFTP (docker compose up --build -d)..."
    "${COMPOSE[@]}" up --build -d
    wait_healthy
    cat <<EOF

Ready:
  App API      $BASE_URL
  Health       $BASE_URL/actuator/health
  Temporal UI  http://localhost:8233
  SFTP         localhost:2222  (user batch / batch)

Run patterns:  scripts/run-patterns.sh
Stop:          scripts/run-project.sh down
EOF
    ;;
  down|stop)
    echo "Stopping stack..."
    "${COMPOSE[@]}" down
    ;;
  status)
    "${COMPOSE[@]}" ps
    echo
    curl -sS "$BASE_URL/actuator/health" | python3 -m json.tool 2>/dev/null \
      || echo "App not reachable at $BASE_URL"
    ;;
  logs)
    "${COMPOSE[@]}" logs -f app
    ;;
  *)
    echo "Unknown command: $cmd" >&2
    usage
    exit 1
    ;;
esac
