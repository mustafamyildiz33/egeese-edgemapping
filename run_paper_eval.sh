#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi
MODE="all"
DURATION="60"
NODES="49"
BATCHES="1"
BATCH_START="1"
FULL_RUN="0"
DRY_RUN="0"
OPEN_REPORT="0"
OPEN_LIVE="0"
BASE_PORT="${EGESS_BASE_PORT:-9000}"
DATA_ONLY="0"
LEAN_GRAPHS="0"
PROOF_LITE="0"
SKIP_DISK_CHECK="0"
START_DELAY_MIN="${EGESS_START_DELAY_MIN:-0}"
FULL_EVIDENCE="${EGESS_FULL_EVIDENCE:-0}"
LOG_MODE="${EGESS_NODE_LOG_MODE:-bounded}"
LOG_MAX_BYTES="${EGESS_NODE_LOG_MAX_BYTES:-16384}"
LOG_MAX_BYTES_SET="0"
MIN_FREE_MB="${EGESS_MIN_FREE_MB:-1024}"
REPORT_BASE_MB="${EGESS_REPORT_BASE_MB:-4}"
REPORT_KB_PER_NODE_SEC="${EGESS_REPORT_KB_PER_NODE_SEC:-2}"

usage() {
  cat <<'EOF'
Usage: ./run_paper_eval.sh [--mode all|phase1|phase2|phase3|phase4] [--duration 60|120] [--nodes N] [--base-port PORT] [--batches N] [--batch-start N] [--start-delay-min N] [--full] [--dry-run] [--open] [--open-live] [--lean-graphs] [--proof-lite] [--data-only] [--log-max-kb N] [--no-node-logs] [--full-logs] [--full-evidence] [--skip-disk-check]

Examples:
  ./run_paper_eval.sh
  ./run_paper_eval.sh --mode all --duration 60 --batches 1 --nodes 49
  ./run_paper_eval.sh --mode phase2 --duration 120 --batches 1 --nodes 49
  ./run_paper_eval.sh --mode phase4 --duration 60 --batches 1 --nodes 49
  ./run_paper_eval.sh --mode all --duration 60 --full --nodes 49

Safe logging defaults:
  Node logs are bounded to 16 KB per node by default.
  Use --log-max-kb N to change the cap, --no-node-logs to disable node logs,
  or --full-logs only when you intentionally want uncapped raw logs.
  Evidence JSON is compact by default; --full-evidence keeps raw node state and
  should only be used for short debugging runs.
  Use --lean-graphs for GitHub/paper collection: it keeps final dashboards and
  comparison figure exports while skipping logs and per-run debug dashboards.
  Use --proof-lite when you want final graphs plus a small proof trail:
  capped 32 KB node logs and compact evidence, without full raw node state.
  Use --data-only for the smallest output: it keeps TSV/JSONL/compact JSON
  metrics and skips logs, per-run dashboards, live HTML, and figure exports.
  Use --start-delay-min N to stagger school-lab starts on a shared server.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="$2"
      shift 2
      ;;
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --nodes)
      NODES="$2"
      shift 2
      ;;
    --base-port)
      BASE_PORT="$2"
      shift 2
      ;;
    --batches)
      BATCHES="$2"
      shift 2
      ;;
    --batch-start)
      BATCH_START="$2"
      shift 2
      ;;
    --full)
      FULL_RUN="1"
      shift
      ;;
    --dry-run)
      DRY_RUN="1"
      shift
      ;;
    --open)
      OPEN_REPORT="1"
      shift
      ;;
    --open-live)
      OPEN_LIVE="1"
      shift
      ;;
    --data-only)
      DATA_ONLY="1"
      shift
      ;;
    --lean-graphs)
      LEAN_GRAPHS="1"
      shift
      ;;
    --proof-lite)
      PROOF_LITE="1"
      shift
      ;;
    --skip-disk-check)
      SKIP_DISK_CHECK="1"
      shift
      ;;
    --start-delay-min)
      START_DELAY_MIN="$2"
      shift 2
      ;;
    --log-max-kb)
      LOG_MAX_BYTES="$(( $2 * 1024 ))"
      LOG_MAX_BYTES_SET="1"
      shift 2
      ;;
    --no-node-logs)
      LOG_MODE="none"
      shift
      ;;
    --full-logs)
      LOG_MODE="full"
      shift
      ;;
    --full-evidence)
      FULL_EVIDENCE="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${PYTHON_BIN}" ]]; then
  echo "Missing python3 interpreter" >&2
  exit 1
fi

if [[ "${DURATION}" != "60" && "${DURATION}" != "120" ]]; then
  echo "--duration must be 60 or 120" >&2
  exit 1
fi

if ! [[ "${BASE_PORT}" == <-> ]]; then
  echo "--base-port must be a positive integer" >&2
  exit 1
fi

if ! [[ "${BATCHES}" == <-> ]]; then
  echo "--batches must be a positive integer" >&2
  exit 1
fi
if ! [[ "${BATCH_START}" == <-> ]]; then
  echo "--batch-start must be a positive integer" >&2
  exit 1
fi
if (( BATCH_START < 1 )); then
  echo "--batch-start must be >= 1" >&2
  exit 1
fi

if [[ "${PROOF_LITE}" == "1" && "${LOG_MAX_BYTES_SET}" != "1" ]]; then
  LOG_MAX_BYTES="32768"
fi

if ! [[ "${LOG_MAX_BYTES}" == <-> ]]; then
  echo "--log-max-kb must be a positive integer" >&2
  exit 1
fi
if (( LOG_MAX_BYTES < 4096 )); then
  LOG_MAX_BYTES="4096"
fi
if ! [[ "${START_DELAY_MIN}" == <-> ]]; then
  echo "--start-delay-min must be a non-negative integer" >&2
  exit 1
fi

node_sum=0
node_max=0
node_entries=0
for item in ${(s:,:)NODES}; do
  [[ -z "${item}" ]] && continue
  if ! [[ "${item}" == <-> ]]; then
    echo "--nodes must be a comma-separated list of integers, got: ${NODES}" >&2
    exit 1
  fi
  node_sum=$((node_sum + item))
  if (( item > node_max )); then
    node_max=$item
  fi
  node_entries=$((node_entries + 1))
done
if (( node_entries < 1 )); then
  echo "--nodes must include at least one node count" >&2
  exit 1
fi

if [[ "${LEAN_GRAPHS}" == "1" ]]; then
  LOG_MODE="none"
  OPEN_LIVE="0"
  REPORT_BASE_MB="${EGESS_REPORT_BASE_MB:-1}"
  REPORT_KB_PER_NODE_SEC="${EGESS_REPORT_KB_PER_NODE_SEC:-0}"
  export EGESS_WRITE_RUN_HTML="0"
  export EGESS_WRITE_LIVE_HTML="0"
  export EGESS_WRITE_RUN_FIGURES="0"
  export EGESS_WRITE_SUITE_FIGURES="1"
  export EGESS_WRITE_PNG_FIGURES="1"
fi

if [[ "${PROOF_LITE}" == "1" ]]; then
  LOG_MODE="bounded"
  FULL_EVIDENCE="0"
  OPEN_LIVE="0"
  if [[ "${LOG_MAX_BYTES_SET}" != "1" ]]; then
    LOG_MAX_BYTES="32768"
  fi
  REPORT_BASE_MB="${EGESS_REPORT_BASE_MB:-2}"
  REPORT_KB_PER_NODE_SEC="${EGESS_REPORT_KB_PER_NODE_SEC:-0}"
  export EGESS_EVIDENCE_RECENT_MSG_LIMIT="${EGESS_EVIDENCE_RECENT_MSG_LIMIT:-8}"
  export EGESS_EVIDENCE_RECENT_ALERT_LIMIT="${EGESS_EVIDENCE_RECENT_ALERT_LIMIT:-8}"
  export EGESS_WRITE_RUN_HTML="0"
  export EGESS_WRITE_LIVE_HTML="0"
  export EGESS_WRITE_RUN_FIGURES="0"
  export EGESS_WRITE_SUITE_FIGURES="1"
  export EGESS_WRITE_PNG_FIGURES="1"
fi

if [[ "${DATA_ONLY}" == "1" ]]; then
  LOG_MODE="none"
  OPEN_LIVE="0"
  REPORT_BASE_MB="${EGESS_REPORT_BASE_MB:-1}"
  REPORT_KB_PER_NODE_SEC="${EGESS_REPORT_KB_PER_NODE_SEC:-0}"
  export EGESS_WRITE_RUN_HTML="0"
  export EGESS_WRITE_LIVE_HTML="0"
  export EGESS_WRITE_RUN_FIGURES="0"
  export EGESS_WRITE_SUITE_FIGURES="0"
  export EGESS_WRITE_PNG_FIGURES="0"
fi

export EGESS_NODE_LOG_MODE="${LOG_MODE}"
export EGESS_NODE_LOG_MAX_BYTES="${LOG_MAX_BYTES}"
export EGESS_LOG="${EGESS_LOG:-0}"
export EGESS_FULL_EVIDENCE="${FULL_EVIDENCE}"
export EGESS_BASE_PORT="${BASE_PORT}"
export EGESS_HISTORY_SCOPE="${EGESS_HISTORY_SCOPE:-watch}"
export EGESS_HTML_REPLAY="${EGESS_HTML_REPLAY:-0}"
export EGESS_HTML_NODE_LOG_LINES="${EGESS_HTML_NODE_LOG_LINES:-0}"
export EGESS_PRETTY_JSON="${EGESS_PRETTY_JSON:-0}"
export EGESS_WRITE_RUN_HTML="${EGESS_WRITE_RUN_HTML:-1}"
export EGESS_WRITE_LIVE_HTML="${EGESS_WRITE_LIVE_HTML:-1}"
export EGESS_WRITE_RUN_FIGURES="${EGESS_WRITE_RUN_FIGURES:-0}"
export EGESS_WRITE_SUITE_FIGURES="${EGESS_WRITE_SUITE_FIGURES:-1}"
export EGESS_WRITE_PNG_FIGURES="${EGESS_WRITE_PNG_FIGURES:-1}"

preflight_disk_check() {
  [[ "${SKIP_DISK_CHECK}" == "1" ]] && return 0
  if [[ "${LOG_MODE}" == "full" ]]; then
    echo "ERROR: --full-logs disables the storage guard and can fill the disk." >&2
    echo "Use the default bounded logs, --no-node-logs, or set --skip-disk-check only if you truly mean it." >&2
    exit 1
  fi
  local scenarios="1"
  [[ "${MODE}" == "all" ]] && scenarios="4"
  local effective_batches="${BATCHES}"
  [[ "${FULL_RUN}" == "1" ]] && effective_batches="30"
  local per_node_bytes="${LOG_MAX_BYTES}"
  [[ "${LOG_MODE}" == "none" || "${LOG_MODE}" == "off" ]] && per_node_bytes="0"
  local run_count=$((node_entries * scenarios * effective_batches))
  local estimated_logs_bytes=$((node_sum * scenarios * effective_batches * per_node_bytes))
  local per_run_report_bytes=$(((REPORT_BASE_MB * 1024 * 1024) + (node_max * DURATION * REPORT_KB_PER_NODE_SEC * 1024)))
  if [[ "${FULL_EVIDENCE}" == "1" ]]; then
    per_run_report_bytes=$((per_run_report_bytes * 3))
  fi
  local estimated_report_bytes=$((run_count * per_run_report_bytes))
  local min_free_bytes=$((MIN_FREE_MB * 1024 * 1024))
  local free_kb
  free_kb="$(df -k "${ROOT_DIR}" | awk 'NR==2 {print $4}')"
  local free_bytes=$((free_kb * 1024))
  local required_bytes=$((estimated_logs_bytes + estimated_report_bytes + min_free_bytes))
  local estimated_mb=$((estimated_logs_bytes / 1024 / 1024))
  local estimated_report_mb=$((estimated_report_bytes / 1024 / 1024))
  echo "Storage guard: node logs capped at ${LOG_MAX_BYTES} bytes each (${estimated_mb} MB worst-case logs for this command)."
  echo "Storage guard: estimated reports/history/evidence ${estimated_report_mb} MB for ${run_count} run(s), plus ${MIN_FREE_MB} MB reserve."
  if [[ "${FULL_EVIDENCE}" == "1" ]]; then
    echo "WARNING: --full-evidence is enabled. This keeps raw node state and can grow quickly." >&2
  fi
  if (( free_bytes < required_bytes )); then
    echo "ERROR: Not enough free disk space for a safe run." >&2
    echo "Free: $((free_bytes / 1024 / 1024)) MB, required: $((required_bytes / 1024 / 1024)) MB including ${MIN_FREE_MB} MB reserve." >&2
    echo "Clean old outputs with: ./clean_eval_outputs.sh runs" >&2
    exit 1
  fi
}

preflight_disk_check

if (( START_DELAY_MIN > 0 )) && [[ "${DRY_RUN}" != "1" ]]; then
  echo "Lab stagger: waiting ${START_DELAY_MIN} minute(s) before starting this run."
  sleep $((START_DELAY_MIN * 60))
fi

common_args=()
if [[ "${DRY_RUN}" == "1" ]]; then
  common_args+=(--dry-run)
fi

if [[ "${MODE}" == "all" ]]; then
  spec="${ROOT_DIR}/paper_eval/campaign/all_together_${DURATION}s.json"
  cmd=("${PYTHON_BIN}" "${ROOT_DIR}/paper_eval_campaign.py" --spec "${spec}" --node-counts "${NODES}")
  cmd+=(--base-port "${BASE_PORT}")
  if [[ "${FULL_RUN}" != "1" ]]; then
    cmd+=(--max-batches "${BATCHES}")
  fi
  cmd+=(--batch-start "${BATCH_START}")
  if [[ "${OPEN_LIVE}" == "1" ]]; then
    cmd+=(--open-live)
  fi
  cmd+=("${common_args[@]}")
  "${cmd[@]}"
  if [[ "${OPEN_REPORT}" == "1" && "${DRY_RUN}" != "1" ]]; then
    latest_dir="$(ls -1dt "${ROOT_DIR}"/campaign_reports/all_together_"${DURATION}"s_*_p"${BASE_PORT}" 2>/dev/null | head -n 1)"
    [[ -n "${latest_dir}" ]] && open "${latest_dir}/index.html"
  fi
  exit 0
fi

case "${MODE}" in
  phase1)
    spec="${ROOT_DIR}/paper_eval/phase1/phase1_baseline_${DURATION}s.json"
    ;;
  phase2)
    spec="${ROOT_DIR}/paper_eval/phase2/phase2_fire_${DURATION}s.json"
    ;;
  phase3)
    spec="${ROOT_DIR}/paper_eval/phase3/phase3_hazard_${DURATION}s.json"
    ;;
  phase4)
    spec="${ROOT_DIR}/paper_eval/phase4/phase4_stress_${DURATION}s.json"
    ;;
  *)
    echo "--mode must be one of: all, phase1, phase2, phase3, phase4" >&2
    exit 1
    ;;
esac

cmd=("${PYTHON_BIN}" "${ROOT_DIR}/paper_eval_runner.py" --spec "${spec}" --node-counts "${NODES}")
cmd+=(--base-port "${BASE_PORT}")
if [[ "${FULL_RUN}" != "1" ]]; then
  cmd+=(--max-runs "${BATCHES}")
fi
cmd+=(--batch-start "${BATCH_START}")
cmd+=("${common_args[@]}")
"${cmd[@]}"

if [[ "${OPEN_REPORT}" == "1" && "${DRY_RUN}" != "1" ]]; then
  latest_dir="$(ls -1dt "${ROOT_DIR}"/paper_reports/*_p"${BASE_PORT}" 2>/dev/null | head -n 1)"
  [[ -n "${latest_dir}" ]] && open "${latest_dir}/index.html"
fi
