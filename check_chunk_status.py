#!/usr/bin/env python3
"""Check one 6-batch paper-eval chunk before starting the next one."""

import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_PHASES = ("phase1", "phase2", "phase3", "phase4")
PASS_STATUSES = {"OK", "WARN"}
ACTIVE_STATUSES = {"PLANNED", "RUNNING"}
FAIL_STATUSES = {"FAILED", "ERROR"}
REQUIRED_SHEETS = (
    "all_runs.csv",
    "all_watch_nodes.csv",
    "summary_by_nodes.csv",
    "metric_averages.csv",
)


def _read_tsv(path):
    with open(path, "r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def _read_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _to_int(value, default=None):
    try:
        if value in ("", None):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_float(value):
    try:
        if value in ("", None):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _mean(values):
    values = [float(value) for value in values if value is not None]
    if not values:
        return ""
    return "{:.3f}".format(statistics.mean(values))


def _parse_int_list(value):
    if not value:
        return []
    out = []
    for item in str(value).split(","):
        item = item.strip()
        if item:
            out.append(int(item))
    return out


def _latest_campaign_dir(root, duration_sec, base_port, batch_start=None):
    campaign_root = root / "campaign_reports"
    if not campaign_root.exists():
        return None
    if base_port:
        pattern = "all_together_{}s_*_p{}*".format(duration_sec, base_port)
    else:
        pattern = "all_together_{}s_*".format(duration_sec)
    candidates = [path for path in campaign_root.glob(pattern) if path.is_dir()]
    if batch_start is not None:
        matching = []
        for path in candidates:
            try:
                found_start = _campaign_batch_start(path)
            except Exception:
                continue
            if found_start == int(batch_start):
                matching.append(path)
        candidates = matching
    if not candidates:
        return None
    return sorted(candidates, key=lambda path: path.stat().st_mtime, reverse=True)[0]


def _load_manifest(campaign_dir):
    dry_path = campaign_dir / "dry_run_manifest.json"
    real_path = campaign_dir / "campaign_manifest.json"
    if dry_path.exists():
        payload = _read_json(dry_path)
        payload["_kind"] = "dry_run"
        return payload
    if real_path.exists():
        payload = _read_json(real_path)
        payload["_kind"] = "real"
        return payload
    return {"_kind": "unknown"}


def _campaign_batch_start(campaign_dir):
    manifest = _load_manifest(campaign_dir)
    found = _to_int(manifest.get("batch_start"), None)
    if found is not None:
        return found
    campaign_tsv = campaign_dir / "campaign_runs.tsv"
    if campaign_tsv.exists():
        rows = _read_tsv(campaign_tsv)
        batches = _int_field_set(rows, "batch_index")
        if batches:
            return min(batches)
    return None


def _field_set(rows, field):
    return sorted({str(row.get(field, "")).strip() for row in rows if str(row.get(field, "")).strip()})


def _int_field_set(rows, field):
    return sorted({_to_int(row.get(field)) for row in rows if _to_int(row.get(field)) is not None})


def _status_counts(rows):
    counts = defaultdict(int)
    for row in rows:
        counts[str(row.get("status", "")).strip().upper() or "BLANK"] += 1
    return dict(sorted(counts.items()))


def _check_campaign_rows(rows, expected_start, expected_batches, expected_nodes, expected_phases, dry_run, issues, warnings):
    expected_end = int(expected_start) + int(expected_batches) - 1
    expected_rows = int(expected_batches) * len(expected_nodes) * len(expected_phases)
    batches = _int_field_set(rows, "batch_index")
    nodes = _int_field_set(rows, "nodes")
    phases = _field_set(rows, "phase_id")
    seeds = _int_field_set(rows, "seed")

    if len(rows) != expected_rows:
        issues.append("campaign_runs.tsv has {} row(s), expected {}".format(len(rows), expected_rows))
    if batches != list(range(expected_start, expected_end + 1)):
        issues.append("batch range is {}, expected {}-{}".format(batches, expected_start, expected_end))
    if nodes != sorted(expected_nodes):
        issues.append("node counts are {}, expected {}".format(nodes, sorted(expected_nodes)))
    if phases != sorted(expected_phases):
        issues.append("phases are {}, expected {}".format(phases, sorted(expected_phases)))
    expected_seed_min = 1000 + expected_start - 1
    expected_seed_max = 1000 + expected_end - 1
    if seeds and (min(seeds) != expected_seed_min or max(seeds) != expected_seed_max):
        issues.append("seed range is {}-{}, expected {}-{}".format(min(seeds), max(seeds), expected_seed_min, expected_seed_max))

    combos = defaultdict(int)
    for row in rows:
        key = (_to_int(row.get("batch_index")), _to_int(row.get("nodes")), str(row.get("phase_id", "")).strip())
        combos[key] += 1
    missing = []
    duplicates = []
    for batch_index in range(expected_start, expected_end + 1):
        for node in expected_nodes:
            for phase in expected_phases:
                count = combos.get((batch_index, node, phase), 0)
                if count == 0:
                    missing.append((batch_index, node, phase))
                elif count > 1:
                    duplicates.append((batch_index, node, phase, count))
    if missing:
        issues.append("missing batch/node/phase rows: {}".format(missing[:8]))
    if duplicates:
        issues.append("duplicate batch/node/phase rows: {}".format(duplicates[:8]))

    counts = _status_counts(rows)
    if dry_run:
        bad = [status for status in counts if status != "PLANNED"]
        if bad:
            issues.append("dry run statuses should be PLANNED only, got {}".format(counts))
    else:
        active = [status for status in counts if status in ACTIVE_STATUSES]
        failed = [status for status in counts if status in FAIL_STATUSES or status == "BLANK"]
        unknown = [status for status in counts if status not in PASS_STATUSES and status not in ACTIVE_STATUSES and status not in FAIL_STATUSES and status != "BLANK"]
        if active:
            issues.append("chunk is not finished yet; active statuses found {}".format({status: counts[status] for status in active}))
        if failed:
            issues.append("failed/blank statuses found {}".format({status: counts[status] for status in failed}))
        if unknown:
            warnings.append("unrecognized statuses found {}".format({status: counts[status] for status in unknown}))

    return {
        "expected_end": expected_end,
        "expected_rows": expected_rows,
        "batches": batches,
        "nodes": nodes,
        "phases": phases,
        "seeds": seeds,
        "statuses": counts,
    }


def _scenario_report_path(raw_path, root):
    path = Path(str(raw_path))
    if path.is_absolute():
        return path
    return (root / path).resolve()


def _group_report_trends(label, rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[_to_int(row.get("nodes"))].append(row)
    lines = []
    for node, node_rows in sorted(grouped.items()):
        if node is None:
            continue
        total_mb = [_to_float(row.get("total_mb")) for row in node_rows]
        tx_fail = [_to_float(row.get("tx_fail_total")) for row in node_rows]
        tx_timeout = [_to_float(row.get("tx_timeout_total")) for row in node_rows]
        detection = [_to_float(row.get("detection_speed_sec")) for row in node_rows]
        accuracy = [_to_float(row.get("settle_accuracy_pct")) for row in node_rows]
        lines.append(
            "{:<22} nodes {:>2}: rows={} avg_mb={} avg_fail={} avg_timeout={} avg_detect={} avg_accuracy={}".format(
                label[:22],
                node,
                len(node_rows),
                _mean(total_mb),
                _mean(tx_fail),
                _mean(tx_timeout),
                _mean(detection),
                _mean(accuracy),
            )
        )
    return lines


def _check_scenario_reports(campaign_dir, manifest, root, expected_batches, expected_nodes, issues, warnings):
    if manifest.get("_kind") == "dry_run":
        return [], "dry run; scenario reports are not generated"
    reports = manifest.get("scenario_reports", {})
    if not reports:
        issues.append("campaign_manifest.json has no scenario_reports")
        return [], ""

    expected_rows = int(expected_batches) * len(expected_nodes)
    trends = []
    checked = 0
    for label, raw_path in sorted(reports.items()):
        report_dir = _scenario_report_path(raw_path, root)
        checked += 1
        if not report_dir.exists():
            issues.append("{} report directory is missing: {}".format(label, report_dir))
            continue
        for rel_path in ("index.html", "all_runs.tsv", "all_watch_nodes.tsv", "summary_by_nodes.tsv", "metric_averages.tsv"):
            path = report_dir / rel_path
            if not path.exists() or path.stat().st_size == 0:
                issues.append("{} missing or empty {}".format(label, rel_path))

        all_runs_path = report_dir / "all_runs.tsv"
        if all_runs_path.exists():
            rows = _read_tsv(all_runs_path)
            if len(rows) != expected_rows:
                issues.append("{} all_runs.tsv has {} row(s), expected {}".format(label, len(rows), expected_rows))
            bad_statuses = _status_counts(rows)
            bad_statuses = {status: count for status, count in bad_statuses.items() if status not in PASS_STATUSES}
            if bad_statuses:
                issues.append("{} all_runs.tsv has bad statuses {}".format(label, bad_statuses))
            for row in rows:
                for field in ("total_mb", "events_total", "reachable_nodes", "status"):
                    if str(row.get(field, "")).strip() == "":
                        issues.append("{} all_runs.tsv has blank {}".format(label, field))
                        break
            trends.extend(_group_report_trends(label, rows))

        figure_dir = report_dir / "figure_exports"
        if not figure_dir.exists():
            issues.append("{} is missing figure_exports/".format(label))
        else:
            png_count = len(list(figure_dir.glob("*.png")))
            tsv_count = len(list(figure_dir.glob("*.tsv")))
            if png_count == 0:
                issues.append("{} has no figure PNGs".format(label))
            if tsv_count == 0:
                issues.append("{} has no figure TSV data".format(label))

        sheet_dir = report_dir / "google_sheets"
        if not sheet_dir.exists():
            issues.append("{} is missing google_sheets/".format(label))
        else:
            for filename in REQUIRED_SHEETS:
                path = sheet_dir / filename
                if not path.exists() or path.stat().st_size == 0:
                    issues.append("{} missing or empty google_sheets/{}".format(label, filename))

    return trends, "{} scenario report(s) checked".format(checked)


def main():
    parser = argparse.ArgumentParser(description="Check one chunk of paper-eval results before running the next chunk")
    parser.add_argument("campaign_dir", nargs="?", help="Optional campaign report directory. Defaults to latest matching campaign.")
    parser.add_argument("--root", default=str(ROOT_DIR), help="Repo root to inspect, defaults to this EGESS repo")
    parser.add_argument("--base-port", type=int, default=9100, help="Base port used for this chunk")
    parser.add_argument("--duration-sec", type=int, default=60, help="Scenario duration, usually 60")
    parser.add_argument("--batch-start", type=int, help="Expected first batch index, e.g. 1, 7, 13, 19, 25")
    parser.add_argument("--batches", type=int, default=6, help="Expected number of batches in this chunk")
    parser.add_argument("--nodes", default="49,64", help="Comma-separated expected node counts")
    parser.add_argument("--phases", default=",".join(DEFAULT_PHASES), help="Comma-separated expected phase IDs")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    if args.campaign_dir:
        campaign_dir = Path(args.campaign_dir).resolve()
    else:
        campaign_dir = _latest_campaign_dir(root, args.duration_sec, args.base_port, batch_start=args.batch_start)
        if campaign_dir is None:
            print("FAIL: no matching campaign report found under {}".format(root / "campaign_reports"), file=sys.stderr)
            return 1

    campaign_tsv = campaign_dir / "campaign_runs.tsv"
    if not campaign_tsv.exists():
        print("FAIL: missing {}".format(campaign_tsv), file=sys.stderr)
        return 1

    rows = _read_tsv(campaign_tsv)
    manifest = _load_manifest(campaign_dir)
    expected_nodes = _parse_int_list(args.nodes) or [int(value) for value in manifest.get("node_counts", [])]
    expected_phases = [item.strip() for item in str(args.phases).split(",") if item.strip()]
    expected_batches = int(args.batches or manifest.get("run_count", 6))
    expected_start = args.batch_start or _to_int(manifest.get("batch_start"), None)
    if expected_start is None and rows:
        expected_start = min(_int_field_set(rows, "batch_index"))
    if expected_start is None:
        expected_start = 1

    issues = []
    warnings = []
    dry_run = manifest.get("_kind") == "dry_run"
    plan = _check_campaign_rows(
        rows=rows,
        expected_start=int(expected_start),
        expected_batches=expected_batches,
        expected_nodes=expected_nodes,
        expected_phases=expected_phases,
        dry_run=dry_run,
        issues=issues,
        warnings=warnings,
    )
    trends, report_note = _check_scenario_reports(
        campaign_dir=campaign_dir,
        manifest=manifest,
        root=root,
        expected_batches=expected_batches,
        expected_nodes=expected_nodes,
        issues=issues,
        warnings=warnings,
    )

    print("Chunk: {}".format(campaign_dir))
    print(
        "Plan: rows={} expected={} actual_batches={} expected_batches={} nodes={} phases={} seeds={}".format(
            len(rows),
            plan["expected_rows"],
            "{}-{}".format(min(plan["batches"]), max(plan["batches"])) if plan["batches"] else "",
            "{}-{}".format(int(expected_start), plan["expected_end"]),
            ",".join(str(value) for value in plan["nodes"]),
            ",".join(plan["phases"]),
            "{}-{}".format(min(plan["seeds"]), max(plan["seeds"])) if plan["seeds"] else "",
        )
    )
    print("Statuses: {}".format(plan["statuses"]))
    if report_note:
        print("Reports: {}".format(report_note))
    if trends:
        print("Quick trends:")
        for line in trends:
            print("  {}".format(line))

    for warning in warnings:
        print("WARN: {}".format(warning), file=sys.stderr)
    if issues:
        for issue in issues:
            print("FAIL: {}".format(issue), file=sys.stderr)
        return 1

    next_start = plan["expected_end"] + 1
    if next_start <= 30:
        print("OK: chunk is safe to continue. Next chunk starts with --batch-start {}".format(next_start))
    else:
        print("OK: chunk is safe. This reaches batch 30, so merge the full dashboard next.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
