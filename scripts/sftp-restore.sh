#!/usr/bin/env bash
# Restore SFTP files: move everything from sftp_data/incoming back to
# sftp_data/home so no file is lost after a demo run.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOME_DIR="$ROOT/sftp_data/home"
IN_DIR="$ROOT/sftp_data/incoming"
mkdir -p "$HOME_DIR"
shopt -s nullglob
for p in "$IN_DIR"/*; do
  b="$(basename "$p")"
  [ "$b" = ".gitkeep" ] && continue
  mv "$p" "$HOME_DIR/$b"; echo "restored: $b -> sftp_data/home/"
done
