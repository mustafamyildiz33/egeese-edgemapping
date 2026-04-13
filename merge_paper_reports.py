#!/usr/bin/env python3
"""Merge chunked paper-eval suite reports into complete suite dashboards."""

import argparse
import re
import sys
import time
import zipfile
from html import escape
from pathlib import Path

import paper_eval_runner as runner


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_REPORT_ROOT = ROOT_DIR / "paper_reports"
MERGED_REPORTS_DIR = ROOT_DIR / "merged_paper_reports"
PORT_RE = re.compile(r"_p([0-9]+)(?:$|_)")


OVERVIEW_FIELDS = [
    "source",
    "scenario",
    "phase_id",
    "challenge",
    "nodes",
    "duration_sec",
    "batches",
    "runs",
    "missing_batches",
    "dashboard",
]


COMBINED_PREFIX_FIELDS = ["source", "scenario", "dashboard"]
COMBINED_RUN_FIELDS = COMBINED_PREFIX_FIELDS + runner.SUMMARY_FIELDS
COMBINED_WATCH_FIELDS = COMBINED_PREFIX_FIELDS + runner.WATCH_FIELDS
COMBINED_NODE_SUMMARY_FIELDS = COMBINED_PREFIX_FIELDS + runner.SUMMARY_BY_NODES_FIELDS
COMBINED_METRIC_FIELDS = COMBINED_PREFIX_FIELDS + ["metric", "field", "samples", "avg", "min", "max", "latest"]


def _parse_root(value):
    if "=" in str(value):
        label, path = str(value).split("=", 1)
        return label.strip() or Path(path).name, Path(path).expanduser()
    path = Path(value).expanduser()
    return path.name or "paper_reports", path


def _suite_dirs(root):
    root = Path(root)
    if (root / "all_runs.tsv").exists():
        return [root]
    if not root.exists():
        return []
    return sorted(path for path in root.iterdir() if path.is_dir() and (path / "all_runs.tsv").exists())


def _port_from_suite(suite_dir, rows):
    for row in rows:
        base_port = str(row.get("base_port", "")).strip()
        if base_port:
            return base_port
        run_dir = str(row.get("run_dir", "")).strip()
        match = PORT_RE.search(run_dir)
        if match:
            return match.group(1)
    match = PORT_RE.search(suite_dir.name)
    return match.group(1) if match else ""


def _to_int_set(value):
    if not value:
        return None
    items = set()
    for item in str(value).split(","):
        item = item.strip()
        if item:
            items.add(int(item))
    return items or None


def _row_int(row, field, default=0):
    return runner._to_int(row.get(field, default), default)


def _row_float(row, field, default=0.0):
    return runner._to_float(row.get(field, default), default)


def _case_key(row):
    return (_row_int(row, "nodes", 0), _row_int(row, "run_index", 0))


def _watch_rows_by_case(rows):
    by_case = {}
    for row in rows:
        by_case.setdefault(_case_key(row), []).append(row)
    return by_case


def _source_label(root_label, base_port):
    if base_port:
        if root_label in ("paper_reports", "reports", ""):
            return "p{}".format(base_port)
        if "p{}".format(base_port) in root_label:
            return root_label
        return "{} p{}".format(root_label, base_port)
    return root_label or "paper_reports"


def _scenario_label(phase_id, challenge):
    return runner._scenario_label(phase_id, challenge)


def _phase_sort_key(phase_id, challenge):
    phase_order = {"phase1": 1, "phase2": 2, "phase3": 3, "phase4": 4}
    return (phase_order.get(str(phase_id), 99), _scenario_label(phase_id, challenge))


def _group_key(root_label, suite_dir, summary_rows):
    row0 = summary_rows[0]
    base_port = _port_from_suite(suite_dir, summary_rows)
    source = _source_label(root_label, base_port)
    duration = str(row0.get("duration_sec", "")).strip()
    return (
        source,
        base_port,
        str(row0.get("protocol", "")).strip(),
        str(row0.get("phase_id", "")).strip(),
        str(row0.get("challenge", "")).strip(),
        duration,
    )


def _matches_filters(row, suite_base_port, args):
    if args.base_port is not None and str(suite_base_port) != str(args.base_port):
        return False
    if args.duration_sec is not None and int(round(_row_float(row, "duration_sec", 0.0))) != int(args.duration_sec):
        return False
    if args.phase and str(row.get("phase_id", "")).strip() != str(args.phase):
        return False
    if args.challenge and str(row.get("challenge", "")).strip() != str(args.challenge):
        return False
    if args.nodes is not None and _row_int(row, "nodes", 0) not in args.nodes:
        return False
    if args.max_batch is not None and _row_int(row, "run_index", 0) > int(args.max_batch):
        return False
    return True


def _collect_groups(roots, args):
    groups = {}
    for root_label, root in roots:
        for suite_dir in _suite_dirs(root):
            summary_rows = runner._read_tsv_rows(suite_dir / "all_runs.tsv")
            if not summary_rows:
                continue
            suite_base_port = _port_from_suite(suite_dir, summary_rows)
            filtered_summary_rows = [
                row
                for row in summary_rows
                if _matches_filters(row, suite_base_port, args)
            ]
            if not filtered_summary_rows:
                continue
            group_key = _group_key(root_label, suite_dir, filtered_summary_rows)
            group = groups.setdefault(
                group_key,
                {
                    "source": group_key[0],
                    "base_port": group_key[1],
                    "protocol": group_key[2],
                    "phase_id": group_key[3],
                    "challenge": group_key[4],
                    "duration_sec": group_key[5],
                    "phase_name": str(filtered_summary_rows[0].get("phase_name", "")).strip(),
                    "cases": {},
                    "suite_dirs": set(),
                },
            )
            mtime = float(suite_dir.stat().st_mtime)
            watch_rows = runner._read_tsv_rows(suite_dir / "all_watch_nodes.tsv")
            watch_by_case = _watch_rows_by_case(watch_rows)
            group["suite_dirs"].add(str(suite_dir))
            for row in filtered_summary_rows:
                case_key = _case_key(row)
                if not all(case_key):
                    continue
                existing = group["cases"].get(case_key)
                if existing is not None and float(existing["mtime"]) > mtime:
                    continue
                case_watch_rows = [
                    item
                    for item in watch_by_case.get(case_key, [])
                    if args.nodes is None or _row_int(item, "nodes", 0) in args.nodes
                ]
                group["cases"][case_key] = {
                    "mtime": mtime,
                    "summary": row,
                    "watch_rows": case_watch_rows,
                    "suite_dir": str(suite_dir),
                }
    return groups


def _safe_slug(value):
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("_")
    return text or "merged_suite"


def _merged_spec(group, rows, expected_batches=None):
    node_counts = sorted({_row_int(row, "nodes", 0) for row in rows if _row_int(row, "nodes", 0) > 0})
    batch_numbers = sorted({_row_int(row, "run_index", 0) for row in rows if _row_int(row, "run_index", 0) > 0})
    run_count = int(expected_batches or (max(batch_numbers) if batch_numbers else len(batch_numbers)))
    duration = runner._to_int(group.get("duration_sec", 0), 0)
    source_slug = _safe_slug(group.get("source", "source"))
    suite_id = "merged_{}_{}_{}s_{}".format(
        group.get("phase_id") or "phase",
        group.get("challenge") or "scenario",
        duration or "mixed",
        source_slug,
    )
    spec = {
        "suite_id": suite_id,
        "phase_id": group.get("phase_id", ""),
        "phase_name": group.get("phase_name") or _scenario_label(group.get("phase_id", ""), group.get("challenge", "")),
        "protocol": group.get("protocol", ""),
        "challenge": group.get("challenge", ""),
        "duration_sec": duration or group.get("duration_sec", ""),
        "base_port": runner._to_int(group.get("base_port", 0), 0) if group.get("base_port") else "",
        "node_counts": node_counts,
        "run_count": run_count,
    }
    return spec


def _missing_batches(rows, expected_batches):
    if not expected_batches:
        return ""
    present = {_row_int(row, "run_index", 0) for row in rows if _row_int(row, "run_index", 0) > 0}
    missing = [str(value) for value in range(1, int(expected_batches) + 1) if value not in present]
    if not missing:
        return "none"
    if len(missing) <= 8:
        return ", ".join(missing)
    return "{} missing".format(len(missing))


def _with_prefix(row, source, scenario, dashboard):
    out = {
        "source": source,
        "scenario": scenario,
        "dashboard": dashboard,
    }
    out.update(row)
    return out


def _format_bytes(size_bytes):
    size = float(size_bytes)
    units = ["B", "KB", "MB", "GB"]
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit == "B":
                return "{} {}".format(int(size), unit)
            return "{:.1f} {}".format(size, unit)
        size /= 1024.0
    return "{} B".format(int(size_bytes))


def _write_portable_export(
    out_dir,
    overview_rows,
    manifest_path,
    combined_run_rows,
    combined_watch_rows,
    combined_node_summary_rows,
    combined_metric_rows,
):
    export_dir = out_dir / "portable_export"
    export_dir.mkdir(parents=True, exist_ok=True)
    runner._write_csv(export_dir / "merged_overview.csv", overview_rows, OVERVIEW_FIELDS)
    runner._write_csv(export_dir / "combined_all_runs.csv", combined_run_rows, COMBINED_RUN_FIELDS)
    runner._write_csv(export_dir / "combined_watch_nodes.csv", combined_watch_rows, COMBINED_WATCH_FIELDS)
    runner._write_csv(export_dir / "combined_summary_by_nodes.csv", combined_node_summary_rows, COMBINED_NODE_SUMMARY_FIELDS)
    runner._write_csv(export_dir / "combined_metric_averages.csv", combined_metric_rows, COMBINED_METRIC_FIELDS)
    runner._write_text(
        export_dir / "README_IMPORT.md",
        "\n".join(
            [
                "# Portable EGESS Paper Export",
                "",
                "This bundle is designed for moving the merged 30-batch evaluation to a personal laptop.",
                "",
                "Recommended imports:",
                "",
                "- `portable_export/combined_all_runs.csv`: all merged run rows across scenarios.",
                "- `portable_export/combined_watch_nodes.csv`: all watched-node evidence rows.",
                "- `portable_export/combined_summary_by_nodes.csv`: 49-vs-64 grouped summary rows.",
                "- `portable_export/combined_metric_averages.csv`: average/min/max/latest metrics by scenario.",
                "- `portable_export/merged_overview.csv`: dashboard index and missing-batch status.",
                "",
                "Open `index.html` after unzipping to browse the dashboards locally.",
                "The scenario folders contain `figure_exports/` PNG/CSV/TSV figures and `google_sheets/` CSVs.",
            ]
        )
        + "\n",
    )

    bundle_path = out_dir / "{}_portable_export.zip".format(out_dir.name)
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(out_dir.rglob("*")):
            if path == bundle_path or path.suffix.lower() == ".zip":
                continue
            if path.is_dir():
                continue
            archive.write(path, arcname=str(Path(out_dir.name) / path.relative_to(out_dir)))
    return bundle_path


def _render_export_panel(bundle_path):
    size = _format_bytes(bundle_path.stat().st_size) if bundle_path and bundle_path.exists() else "ready"
    href = bundle_path.name if bundle_path else ""
    return """<section class="panel">
<div class="panel-head">
  <h2>Portable Export</h2>
  <p class="section-note">Download one ZIP with the merged dashboards, combined CSVs, Google Sheets files, and paper figures.</p>
</div>
<a class="run-link-action" href="{href}" download>Download Export Bundle</a>
<p class="section-note" style="margin-top:10px;">Bundle size: {size}. Unzip it on your personal laptop and open <code>index.html</code>.</p>
</section>""".format(href=escape(str(href)), size=escape(str(size)))


def _write_merge_index(out_dir, overview_rows, manifest_path, bundle_path=None):
    scenario_count = len(overview_rows)
    total_runs = sum(runner._to_int(row.get("runs", 0), 0) for row in overview_rows)
    cards = [
        {"label": "Merged Suites", "value": str(scenario_count), "note": "scenario dashboards", "tone": "accent"},
        {"label": "Rows", "value": str(total_runs), "note": "deduped runs", "tone": "accent"},
        {"label": "Manifest", "value": manifest_path.name, "note": "source folders and gaps", "tone": "accent"},
    ]
    if bundle_path:
        bundle_value = _format_bytes(bundle_path.stat().st_size) if bundle_path.exists() else "ready"
        cards.append({"label": "Export Bundle", "value": bundle_value, "note": "portable ZIP", "tone": "good"})
    links = [(row["dashboard"], "{} - {}".format(row["source"], row["scenario"])) for row in overview_rows]
    links.append((manifest_path.name, manifest_path.name))
    sections = [
        _render_export_panel(bundle_path) if bundle_path else "",
        runner._render_links_html("Merged Dashboards", links),
        runner._render_table_html(
            "Merged Overview",
            overview_rows,
            OVERVIEW_FIELDS,
            "Each dashboard is rebuilt from the chunk rows, with duplicate reruns replaced by the newest copy.",
        ),
    ]
    runner._write_text(
        out_dir / "index.html",
        runner._html_page(
            "Merged Paper Evaluation Reports",
            "Complete dashboards rebuilt from chunked 6-batch runs",
            runner._render_cards_html(cards),
            "".join(sections),
        ),
    )


def merge_reports(args):
    roots = [_parse_root(item) for item in (args.root or [str(DEFAULT_REPORT_ROOT)])]
    groups = _collect_groups(roots, args)
    if not groups:
        raise ValueError("no suite reports matched the requested filters")

    stamp_bits = [time.strftime("%Y%m%d_%H%M%S")]
    if args.base_port is not None:
        stamp_bits.append("p{}".format(args.base_port))
    stamp = "_".join(stamp_bits)
    out_dir = MERGED_REPORTS_DIR / "merged_{}".format(stamp)
    suffix = 2
    while out_dir.exists():
        out_dir = MERGED_REPORTS_DIR / "merged_{}_r{}".format(stamp, suffix)
        suffix += 1
    out_dir.mkdir(parents=True, exist_ok=True)

    overview_rows = []
    manifest_groups = []
    combined_run_rows = []
    combined_watch_rows = []
    combined_node_summary_rows = []
    combined_metric_rows = []
    for group in sorted(groups.values(), key=lambda item: (item["source"], _phase_sort_key(item["phase_id"], item["challenge"]))):
        cases = group["cases"]
        rows = [
            item["summary"]
            for _, item in sorted(
                cases.items(),
                key=lambda pair: (runner._to_int(pair[0][1], 0), runner._to_int(pair[0][0], 0)),
            )
        ]
        watch_rows = []
        for _, item in sorted(cases.items(), key=lambda pair: (runner._to_int(pair[0][1], 0), runner._to_int(pair[0][0], 0))):
            watch_rows.extend(item["watch_rows"])
        if not rows:
            continue
        spec = _merged_spec(group, rows, expected_batches=args.expected_batches)
        scenario_slug = _safe_slug("{}_{}_{}".format(group["source"], group["phase_id"], group["challenge"]))
        report_dir = out_dir / scenario_slug
        report_dir.mkdir(parents=True, exist_ok=True)
        runner._write_suite_reports(report_dir, spec, rows, watch_rows)

        batch_numbers = sorted({_row_int(row, "run_index", 0) for row in rows if _row_int(row, "run_index", 0) > 0})
        node_counts = sorted({_row_int(row, "nodes", 0) for row in rows if _row_int(row, "nodes", 0) > 0})
        dashboard_href = "{}/index.html".format(report_dir.name)
        scenario_name = _scenario_label(group["phase_id"], group["challenge"])
        combined_run_rows.extend(_with_prefix(row, group["source"], scenario_name, dashboard_href) for row in rows)
        combined_watch_rows.extend(_with_prefix(row, group["source"], scenario_name, dashboard_href) for row in watch_rows)
        combined_node_summary_rows.extend(
            _with_prefix(row, group["source"], scenario_name, dashboard_href)
            for row in runner._suite_summary_rows(rows)
        )
        combined_metric_rows.extend(
            _with_prefix(row, group["source"], scenario_name, dashboard_href)
            for row in runner._metric_summary_rows(rows, runner.SUMMARY_CHART_FIELDS)
        )
        overview_rows.append(
            {
                "source": group["source"],
                "scenario": scenario_name,
                "phase_id": group["phase_id"],
                "challenge": group["challenge"],
                "nodes": ", ".join(str(value) for value in node_counts),
                "duration_sec": group["duration_sec"],
                "batches": "{}-{}".format(min(batch_numbers), max(batch_numbers)) if batch_numbers else "",
                "runs": len(rows),
                "missing_batches": _missing_batches(rows, args.expected_batches),
                "dashboard": dashboard_href,
            }
        )
        manifest_groups.append(
            {
                "source": group["source"],
                "base_port": group["base_port"],
                "phase_id": group["phase_id"],
                "challenge": group["challenge"],
                "duration_sec": group["duration_sec"],
                "nodes": node_counts,
                "batches": batch_numbers,
                "runs": len(rows),
                "missing_batches": _missing_batches(rows, args.expected_batches),
                "dashboard": str(report_dir),
                "source_suite_dirs": sorted(group["suite_dirs"]),
            }
        )

    manifest_path = out_dir / "merge_manifest.json"
    runner._write_json(
        manifest_path,
        {
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "roots": [{"label": label, "path": str(path)} for label, path in roots],
            "filters": {
                "base_port": args.base_port,
                "nodes": sorted(args.nodes) if args.nodes else None,
                "duration_sec": args.duration_sec,
                "phase": args.phase,
                "challenge": args.challenge,
                "expected_batches": args.expected_batches,
                "max_batch": args.max_batch,
            },
            "groups": manifest_groups,
            },
        )
    pending_bundle_path = out_dir / "{}_portable_export.zip".format(out_dir.name)
    _write_merge_index(out_dir, overview_rows, manifest_path, bundle_path=pending_bundle_path)
    bundle_path = _write_portable_export(
        out_dir,
        overview_rows,
        manifest_path,
        combined_run_rows,
        combined_watch_rows,
        combined_node_summary_rows,
        combined_metric_rows,
    )
    _write_merge_index(out_dir, overview_rows, manifest_path, bundle_path=bundle_path)
    return out_dir


def main():
    parser = argparse.ArgumentParser(description="Merge chunked paper-eval suite reports into complete HTML dashboards")
    parser.add_argument("--root", action="append", help="Report root to read, optionally label=/path/to/paper_reports. May be repeated.")
    parser.add_argument("--base-port", type=int, help="Only merge reports from this base port, for example 9100")
    parser.add_argument("--nodes", help="Comma-separated node counts to include, for example 49,64")
    parser.add_argument("--duration-sec", type=int, help="Only merge this duration, for example 60")
    parser.add_argument("--phase", help="Only merge one phase id, for example phase4")
    parser.add_argument("--challenge", help="Only merge one challenge, for example ghost_outage_noise")
    parser.add_argument("--expected-batches", type=int, default=30, help="Expected complete batch count used to report missing batches")
    parser.add_argument("--max-batch", type=int, help="Ignore rows beyond this batch/run index")
    args = parser.parse_args()
    args.nodes = _to_int_set(args.nodes)

    try:
        out_dir = merge_reports(args)
    except Exception as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        sys.exit(1)

    print("Merged report directory: {}".format(out_dir))
    print("Open: {}/index.html".format(out_dir))


if __name__ == "__main__":
    main()
