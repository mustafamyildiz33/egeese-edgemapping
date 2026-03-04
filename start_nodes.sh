#!/bin/bash
set -e

cd "$(dirname "$0")"

BASE_PORT=9000
TOTAL_NODES="${1:-36}"

if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python)"
else
  echo "ERROR: No Python interpreter found (tried .venv/bin/python, python3, python)."
  exit 1
fi

STAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="runs/$STAMP"
mkdir -p "$RUN_DIR"
PIDS_FILE="$RUN_DIR/pids.txt"
: > "$PIDS_FILE"

export DEMO_MODE=1
export EGESS_LOG_DIR="$RUN_DIR"
export EGESS_LOG="${EGESS_LOG:-0}"

echo "Starting $TOTAL_NODES nodes (DEMO_MODE=1) ..."
echo "Python: $PYTHON_BIN"
echo "EGESS_LOG: $EGESS_LOG"
echo "Run dir: $RUN_DIR"

for ((i=0; i<TOTAL_NODES; i++)); do
  PORT=$((BASE_PORT + i))
  nohup "$PYTHON_BIN" -u node.py "$PORT" "$TOTAL_NODES" > "$RUN_DIR/node_${PORT}.log" 2>&1 < /dev/null &
  echo "$!" >> "$PIDS_FILE"
done

echo "Started. Tip: tail -f $RUN_DIR/node_${BASE_PORT}.log"
echo "PIDs: $PIDS_FILE"
echo "$RUN_DIR" > "$RUN_DIR/LATEST_RUN_DIR.txt"
