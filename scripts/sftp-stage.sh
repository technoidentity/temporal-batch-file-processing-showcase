#!/usr/bin/env bash
# Stage SFTP file(s): move from sftp_data/home into sftp_data/incoming so the
# Pattern-1 poller detects and downloads them. Files are MOVED (restore-safe).
# Usage: scripts/sftp-stage.sh [filename]   (no arg = stage all files in home)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOME_DIR="$ROOT/sftp_data/home"
IN_DIR="$ROOT/sftp_data/incoming"
mkdir -p "$IN_DIR"
if [ "$#" -ge 1 ]; then
  files=("$1")
else
  mapfile -t files < <(cd "$HOME_DIR" 2>/dev/null && ls -1)
fi
for f in "${files[@]:-}"; do
  [ -n "$f" ] || continue
  if [ -f "$HOME_DIR/$f" ]; then
    mv "$HOME_DIR/$f" "$IN_DIR/$f"; echo "staged: $f -> sftp_data/incoming/"
  else
    echo "skip (not in home): $f"
  fi
done
