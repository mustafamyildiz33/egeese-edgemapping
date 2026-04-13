#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

SCOPE="${1:-runs}"

case "$SCOPE" in
  runs)
    mkdir -p runs
    find runs -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    echo "Cleared runs/ raw run folders."
    ;;
  reports)
    mkdir -p paper_reports campaign_reports comparison_reports
    find paper_reports -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    find campaign_reports -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    find comparison_reports -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    echo "Cleared generated report folders."
    ;;
  all)
    mkdir -p runs paper_reports campaign_reports comparison_reports
    find runs -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    find paper_reports -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    find campaign_reports -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    find comparison_reports -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    echo "Cleared raw runs and generated report folders."
    ;;
  *)
    echo "Usage: ./clean_eval_outputs.sh [runs|reports|all]" >&2
    exit 2
    ;;
esac
