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
FULL_RUN="0"
DRY_RUN="0"
OPEN_REPORT="0"

usage() {
  cat <<'EOF'
Usage: ./run_paper_eval.sh [--mode all|phase1|phase2|phase3] [--duration 60|120] [--nodes N] [--batches N] [--full] [--dry-run] [--open]

Examples:
  ./run_paper_eval.sh
  ./run_paper_eval.sh --mode all --duration 60 --batches 1 --nodes 49
  ./run_paper_eval.sh --mode phase2 --duration 120 --batches 1 --nodes 49
  ./run_paper_eval.sh --mode all --duration 60 --full --nodes 49
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
    --batches)
      BATCHES="$2"
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

common_args=()
if [[ "${DRY_RUN}" == "1" ]]; then
  common_args+=(--dry-run)
fi

if [[ "${MODE}" == "all" ]]; then
  spec="${ROOT_DIR}/paper_eval/campaign/all_together_${DURATION}s.json"
  cmd=("${PYTHON_BIN}" "${ROOT_DIR}/paper_eval_campaign.py" --spec "${spec}" --node-counts "${NODES}")
  if [[ "${FULL_RUN}" != "1" ]]; then
    cmd+=(--max-batches "${BATCHES}")
  fi
  cmd+=("${common_args[@]}")
  "${cmd[@]}"
  if [[ "${OPEN_REPORT}" == "1" && "${DRY_RUN}" != "1" ]]; then
    latest_dir="$(ls -1dt "${ROOT_DIR}"/campaign_reports/all_together_"${DURATION}"s_* 2>/dev/null | head -n 1)"
    [[ -n "${latest_dir}" ]] && open "${latest_dir}/index.html"
  fi
  exit 0
fi

case "${MODE}" in
  phase1)
    spec="${ROOT_DIR}/paper_eval/phase1/phase1_baseline_${DURATION}s.json"
    ;;
  phase2)
    spec="${ROOT_DIR}/paper_eval/phase2/phase2_hazard_${DURATION}s.json"
    ;;
  phase3)
    spec="${ROOT_DIR}/paper_eval/phase3/phase3_stress_${DURATION}s.json"
    ;;
  *)
    echo "--mode must be one of: all, phase1, phase2, phase3" >&2
    exit 1
    ;;
esac

cmd=("${PYTHON_BIN}" "${ROOT_DIR}/paper_eval_runner.py" --spec "${spec}" --node-counts "${NODES}")
if [[ "${FULL_RUN}" != "1" ]]; then
  cmd+=(--max-runs "${BATCHES}")
fi
cmd+=("${common_args[@]}")
"${cmd[@]}"

if [[ "${OPEN_REPORT}" == "1" && "${DRY_RUN}" != "1" ]]; then
  latest_dir="$(ls -1dt "${ROOT_DIR}"/paper_reports/* 2>/dev/null | head -n 1)"
  [[ -n "${latest_dir}" ]] && open "${latest_dir}/index.html"
fi
