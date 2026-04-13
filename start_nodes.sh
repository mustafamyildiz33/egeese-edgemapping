#!/bin/bash
set -e

cd "$(dirname "$0")"

BASE_PORT="${EGESS_BASE_PORT:-9000}"
TOTAL_NODES="36"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-port)
      BASE_PORT="$2"
      shift 2
      ;;
    --nodes)
      TOTAL_NODES="$2"
      shift 2
      ;;
    *)
      TOTAL_NODES="$1"
      shift
      ;;
  esac
done

if ! [[ "$BASE_PORT" =~ ^[0-9]+$ ]]; then
  echo "ERROR: --base-port must be an integer."
  exit 1
fi

if ! [[ "$TOTAL_NODES" =~ ^[0-9]+$ ]] || [[ "$TOTAL_NODES" -lt 2 ]]; then
  echo "ERROR: --nodes must be an integer >= 2."
  exit 1
fi

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

STAMP="$(date +%Y%m%d_%H%M%S)_p${BASE_PORT}"
RUN_DIR="runs/$STAMP"
mkdir -p "$RUN_DIR"
PIDS_FILE="$RUN_DIR/pids.txt"
: > "$PIDS_FILE"
touch "$RUN_DIR/data.csv"
if [[ "${EGESS_UPDATE_DATA_SYMLINK:-0}" == "1" ]]; then
  ln -sf "$RUN_DIR/data.csv" data.csv
fi

GRID_SIDE=1
while (( GRID_SIDE * GRID_SIDE < TOTAL_NODES )); do
  GRID_SIDE=$((GRID_SIDE + 1))
done

export DEMO_MODE=1
export EGESS_BASE_PORT="$BASE_PORT"
export EGESS_LOG_DIR="$RUN_DIR"
export EGESS_LOG="${EGESS_LOG:-0}"
export EGESS_GRID_SIZE="${EGESS_GRID_SIZE:-$GRID_SIDE}"
export EGESS_NODE_LOG_MODE="${EGESS_NODE_LOG_MODE:-bounded}"
export EGESS_NODE_LOG_MAX_BYTES="${EGESS_NODE_LOG_MAX_BYTES:-16384}"

echo "Starting $TOTAL_NODES nodes (DEMO_MODE=1) ..."
echo "EGESS_BASE_PORT: $EGESS_BASE_PORT"
echo "Python: $PYTHON_BIN"
echo "EGESS_LOG: $EGESS_LOG"
echo "EGESS_NODE_LOG_MODE: $EGESS_NODE_LOG_MODE"
echo "EGESS_NODE_LOG_MAX_BYTES: $EGESS_NODE_LOG_MAX_BYTES"
echo "EGESS_GRID_SIZE: $EGESS_GRID_SIZE"
echo "Run dir: $RUN_DIR"

for ((i=0; i<TOTAL_NODES; i++)); do
  PORT=$((BASE_PORT + i))
  LOG_PATH="$RUN_DIR/node_${PORT}.log"
  : > "$LOG_PATH"
  if [[ "$EGESS_NODE_LOG_MODE" == "full" ]]; then
    nohup "$PYTHON_BIN" -u node.py "$PORT" "$TOTAL_NODES" > "$LOG_PATH" 2>&1 < /dev/null &
  elif [[ "$EGESS_NODE_LOG_MODE" == "none" || "$EGESS_NODE_LOG_MODE" == "off" ]]; then
    nohup "$PYTHON_BIN" -u node.py "$PORT" "$TOTAL_NODES" > /dev/null 2>&1 < /dev/null &
  else
    nohup bash -c '"$1" -u node.py "$2" "$3" 2>&1 | "$1" tools/bounded_log.py "$4" "$5"' _ "$PYTHON_BIN" "$PORT" "$TOTAL_NODES" "$LOG_PATH" "$EGESS_NODE_LOG_MAX_BYTES" < /dev/null &
  fi
  echo "$!" >> "$PIDS_FILE"
done

echo "Started. Tip: tail -f $RUN_DIR/node_${BASE_PORT}.log"
echo "PIDs: $PIDS_FILE"
echo "$RUN_DIR" > "$RUN_DIR/LATEST_RUN_DIR.txt"
