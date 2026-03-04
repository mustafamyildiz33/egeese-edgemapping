#!/bin/bash
set -e

cd "$(dirname "$0")"

LATEST_RUN="$(ls -1t runs 2>/dev/null | head -n 1 || true)"
if [[ -n "$LATEST_RUN" && -f "runs/$LATEST_RUN/pids.txt" ]]; then
  while IFS= read -r pid; do
    kill "$pid" 2>/dev/null || true
  done < "runs/$LATEST_RUN/pids.txt"
fi

pkill -f "node.py [0-9][0-9]* [0-9][0-9]*" 2>/dev/null || true
