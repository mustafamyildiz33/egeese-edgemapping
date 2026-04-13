#!/bin/bash
set -e

cd "$(dirname "$0")"

BASE_PORT=""
RUN_DIR=""
KILL_ALL=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-port)
      BASE_PORT="$2"
      shift 2
      ;;
    --run-dir)
      RUN_DIR="$2"
      shift 2
      ;;
    --all)
      KILL_ALL=1
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: ./stop_nodes.sh [--base-port PORT] [--run-dir runs/<timestamp>] [--all]"
      exit 1
      ;;
  esac
done

if [[ -z "$RUN_DIR" ]]; then
  if [[ -n "$BASE_PORT" ]]; then
    RUN_DIR="$(ls -1dt runs/*_p"${BASE_PORT}" 2>/dev/null | head -n 1 || true)"
  else
    RUN_DIR="$(ls -1dt runs/* 2>/dev/null | head -n 1 || true)"
  fi
fi

if [[ -n "$RUN_DIR" && -f "$RUN_DIR/pids.txt" ]]; then
  while IFS= read -r pid; do
    kill "$pid" 2>/dev/null || true
  done < "$RUN_DIR/pids.txt"
fi

if [[ "$KILL_ALL" -eq 1 ]]; then
  pkill -f "node.py [0-9][0-9]* [0-9][0-9]*" 2>/dev/null || true
fi
