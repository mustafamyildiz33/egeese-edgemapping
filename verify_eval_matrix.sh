#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"
CHECKIN_DIR="${ROOT_DIR}/external/checkin-egess-eval"
CHECKIN_PYTHON_BIN="${CHECKIN_DIR}/.venv/bin/python"
if [[ ! -x "${CHECKIN_PYTHON_BIN}" ]]; then
  CHECKIN_PYTHON_BIN="${PYTHON_BIN}"
fi

assert_plan() {
  local label="$1"
  local file="$2"
  local expected_start="$3"
  local expected_end="$4"
  awk -F'\t' -v label="${label}" -v expected_start="${expected_start}" -v expected_end="${expected_end}" '
    NR == 1 { next }
    {
      rows += 1
      phases[$3] = 1
      nodes[$5] = 1
      if (rows == 1 || $1 < min_batch) min_batch = $1
      if (rows == 1 || $1 > max_batch) max_batch = $1
      if (rows == 1 || $6 < min_seed) min_seed = $6
      if (rows == 1 || $6 > max_seed) max_seed = $6
    }
    END {
      phase_count = 0
      node_count = 0
      for (phase in phases) phase_count += 1
      for (node in nodes) node_count += 1
      if (rows != 48 || phase_count != 4 || node_count != 2 || min_batch != expected_start || max_batch != expected_end) {
        printf("FAIL %s: rows=%s phases=%s nodes=%s batches=%s-%s seeds=%s-%s\n", label, rows, phase_count, node_count, min_batch, max_batch, min_seed, max_seed) > "/dev/stderr"
        exit 1
      }
      printf("OK %s: rows=%s phases=%s nodes=%s batches=%s-%s seeds=%s-%s\n", label, rows, phase_count, node_count, min_batch, max_batch, min_seed, max_seed)
    }
  ' "${file}"
}

latest_campaign_file() {
  local root="$1"
  local port="$2"
  ls -1dt "${root}"/campaign_reports/all_together_60s_*_p"${port}"* 2>/dev/null | head -n 1
}

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Missing Python venv at ${PYTHON_BIN}" >&2
  exit 1
fi
if [[ ! -d "${CHECKIN_DIR}" ]]; then
  echo "Missing Check-In repo at ${CHECKIN_DIR}" >&2
  exit 1
fi

echo "Compiling EGESS runner..."
"${PYTHON_BIN}" -m py_compile "${ROOT_DIR}/paper_eval_runner.py" "${ROOT_DIR}/paper_eval_campaign.py" "${ROOT_DIR}/merge_paper_reports.py" "${ROOT_DIR}/lab_compare.py"
zsh -n "${ROOT_DIR}/run_paper_eval.sh"

echo "Compiling Check-In runner..."
"${CHECKIN_PYTHON_BIN}" -m py_compile "${CHECKIN_DIR}/paper_eval_runner.py" "${CHECKIN_DIR}/paper_eval_campaign.py" "${CHECKIN_DIR}/node.py"
zsh -n "${CHECKIN_DIR}/run_paper_eval.sh"

echo "Dry-running EGESS chunk 1..."
"${ROOT_DIR}/run_paper_eval.sh" --base-port 9100 --mode all --duration 60 --batches 6 --batch-start 1 --nodes 49,64 --lean-graphs --dry-run
egess_chunk1="$(latest_campaign_file "${ROOT_DIR}" 9100)"
assert_plan "EGESS batches 1-6" "${egess_chunk1}/campaign_runs.tsv" 1 6

echo "Dry-running EGESS chunk 2..."
"${ROOT_DIR}/run_paper_eval.sh" --base-port 9100 --mode all --duration 60 --batches 6 --batch-start 7 --nodes 49,64 --lean-graphs --dry-run
egess_chunk2="$(latest_campaign_file "${ROOT_DIR}" 9100)"
assert_plan "EGESS batches 7-12" "${egess_chunk2}/campaign_runs.tsv" 7 12

echo "Dry-running Check-In chunk 1..."
(cd "${CHECKIN_DIR}" && ./run_paper_eval.sh --base-port 9200 --mode all --duration 60 --batches 6 --batch-start 1 --nodes 49,64 --lean-graphs --dry-run)
checkin_chunk1="$(latest_campaign_file "${CHECKIN_DIR}" 9200)"
assert_plan "Check-In batches 1-6" "${checkin_chunk1}/campaign_runs.tsv" 1 6

echo "Dry-running Check-In chunk 2..."
(cd "${CHECKIN_DIR}" && ./run_paper_eval.sh --base-port 9200 --mode all --duration 60 --batches 6 --batch-start 7 --nodes 49,64 --lean-graphs --dry-run)
checkin_chunk2="$(latest_campaign_file "${CHECKIN_DIR}" 9200)"
assert_plan "Check-In batches 7-12" "${checkin_chunk2}/campaign_runs.tsv" 7 12

echo "Preflight OK: EGESS and Check-In both plan 4 phases x nodes 49,64 with unique chunked batches."
