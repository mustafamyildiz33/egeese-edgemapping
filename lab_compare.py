#!/usr/bin/env python3
"""Build a final comparison dashboard across EGESS lab machines/runs.

The full lab workflow can run the same EGESS suite on several computers or
several base ports on one shared server. This post-processor reads the finished
`paper_reports/` folders and creates one compact HTML dashboard with graphs
that compare the final suite-level metrics across those sources.
"""

import argparse
import re
import statistics
import time
from pathlib import Path

from paper_eval_runner import (
    _field_label,
    _format_display_value,
    _html_page,
    _matplotlib_pyplot,
    _maybe_float,
    _read_tsv_rows,
    _render_cards_html,
    _render_links_html,
    _render_table_html,
    _scenario_label,
    _write_text,
    _write_tsv,
)


ROOT_DIR = Path(__file__).resolve().parent
LAB_REPORTS_DIR = ROOT_DIR / "lab_comparison_reports"
PORT_RE = re.compile(r"_p([0-9]+)(?:$|_)")

OVERVIEW_FIELDS = [
    "source",
    "scenario",
    "phase_id",
    "challenge",
    "nodes",
    "duration_sec",
    "runs",
    "avg_total_mb",
    "avg_detection_speed_sec",
    "avg_failures_total",
    "avg_false_positive_nodes",
    "avg_false_unavailable_refs",
    "avg_settle_accuracy_pct",
    "suite_dir",
]

METRIC_SPECS = [
    ("avg_total_mb", "Average Overhead MB", "#ff7a59"),
    ("avg_detection_speed_sec", "Average Detection Latency", "#2474e5"),
    ("avg_failures_total", "Average TX Problems", "#c73a3a"),
    ("avg_false_positive_nodes", "Average False Positive Nodes", "#8b4cd6"),
    ("avg_false_unavailable_refs", "Average False Unavailable Refs", "#c58f10"),
    ("avg_settle_accuracy_pct", "Average Settle Accuracy", "#118a7e"),
]


def _parse_source(value):
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


def _port_suffix(suite_dir, rows):
    match = PORT_RE.search(str(suite_dir.name))
    if match:
        return "p{}".format(match.group(1))
    for row in rows:
        base_port = str(row.get("base_port", "")).strip()
        if base_port:
            return "p{}".format(base_port)
        run_dir = str(row.get("run_dir", ""))
        match = PORT_RE.search(run_dir)
        if match:
            return "p{}".format(match.group(1))
    return ""


def _avg(rows, field):
    values = []
    for row in rows:
        value = _maybe_float(row.get(field))
        if value is not None:
            values.append(float(value))
    return round(statistics.mean(values), 3) if values else ""


def _avg_failures(rows):
    values = []
    for row in rows:
        try:
            values.append(
                float(row.get("tx_fail_total", 0) or 0)
                + float(row.get("tx_timeout_total", 0) or 0)
                + float(row.get("tx_conn_error_total", 0) or 0)
            )
        except Exception:
            continue
    return round(statistics.mean(values), 3) if values else ""


def _case_key(row):
    return (
        str(row.get("nodes", "")).strip(),
        str(row.get("run_index", "")).strip(),
    )


def _collect_suites(sources):
    groups = {}
    for root_label, root in sources:
        for suite_dir in _suite_dirs(root):
            rows = _read_tsv_rows(suite_dir / "all_runs.tsv")
            if not rows:
                continue
            row0 = rows[0]
            scenario = _scenario_label(row0.get("phase_id", ""), row0.get("challenge", ""))
            suffix = _port_suffix(suite_dir, rows)
            source = suffix if root_label in ("paper_reports", "reports", "") and suffix else root_label
            if root_label not in ("paper_reports", "reports", "") and suffix and suffix not in root_label:
                source = "{} {}".format(root_label, suffix)
            signature = (source, str(row0.get("phase_id", "")), str(row0.get("challenge", "")))
            mtime = suite_dir.stat().st_mtime
            group = groups.setdefault(
                signature,
                {
                    "mtime": 0,
                    "source": source,
                    "scenario": scenario,
                    "phase_id": str(row0.get("phase_id", "")),
                    "challenge": str(row0.get("challenge", "")),
                    "rows_by_case": {},
                    "suite_dirs": set(),
                },
            )
            group["mtime"] = max(float(group.get("mtime", 0)), float(mtime))
            group["suite_dirs"].add(str(suite_dir))
            for row in rows:
                case_key = _case_key(row)
                if not all(case_key):
                    case_key = (str(row.get("nodes", "")).strip(), str(row.get("run_dir", "")).strip())
                previous = group["rows_by_case"].get(case_key)
                if previous is None or float(mtime) >= float(previous["mtime"]):
                    group["rows_by_case"][case_key] = {"mtime": float(mtime), "row": row}

    overview_rows = []
    for group in groups.values():
        rows = [
            item["row"]
            for _, item in sorted(
                group["rows_by_case"].items(),
                key=lambda pair: (
                    int(pair[0][0]) if str(pair[0][0]).isdigit() else 0,
                    int(pair[0][1]) if str(pair[0][1]).isdigit() else 0,
                ),
            )
        ]
        node_counts = sorted({str(row.get("nodes", "")).strip() for row in rows if str(row.get("nodes", "")).strip()})
        durations = sorted({str(row.get("duration_sec", "")).strip() for row in rows if str(row.get("duration_sec", "")).strip()})
        suite_dirs = sorted(group["suite_dirs"])
        suite_note = suite_dirs[0] if len(suite_dirs) == 1 else "{} folders".format(len(suite_dirs))
        overview_rows.append(
            {
                "source": group["source"],
                "scenario": group["scenario"],
                "phase_id": group["phase_id"],
                "challenge": group["challenge"],
                "nodes": ", ".join(node_counts),
                "duration_sec": ", ".join(durations),
                "runs": len(rows),
                "avg_total_mb": _avg(rows, "total_mb"),
                "avg_detection_speed_sec": _avg(rows, "detection_speed_sec"),
                "avg_failures_total": _avg_failures(rows),
                "avg_false_positive_nodes": _avg(rows, "false_positive_nodes"),
                "avg_false_unavailable_refs": _avg(rows, "false_unavailable_refs"),
                "avg_settle_accuracy_pct": _avg(rows, "settle_accuracy_pct"),
                "suite_dir": suite_note,
            }
        )
    return sorted(overview_rows, key=lambda row: (row["scenario"], row["source"]))


def _scenario_order(rows):
    preferred = ["Baseline", "Fire", "Tornado", "Ghost Outage + Noise"]
    names = sorted({row["scenario"] for row in rows})
    return [name for name in preferred if name in names] + [name for name in names if name not in preferred]


def _write_metric_chart(out_dir, rows, metric, title, color):
    export_dir = out_dir / "figure_exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    scenario_names = _scenario_order(rows)
    sources = sorted({row["source"] for row in rows})
    value_by_key = {}
    points = []
    for row in rows:
        value = _maybe_float(row.get(metric))
        if value is None:
            continue
        key = (row["scenario"], row["source"])
        value_by_key[key] = float(value)
        points.append({"scenario": row["scenario"], "source": row["source"], "value": round(float(value), 3)})
    tsv_path = export_dir / "{}.tsv".format(metric)
    _write_tsv(tsv_path, points, ["scenario", "source", "value"])
    if not points:
        return ("{}.tsv".format(metric), "figure_exports/{}.tsv".format(metric))

    plt = _matplotlib_pyplot()
    fig_width = max(8.0, 1.6 * max(1, len(scenario_names)))
    fig, ax = plt.subplots(figsize=(fig_width, 4.8), dpi=180)
    x_positions = list(range(len(scenario_names)))
    bar_width = min(0.18, 0.72 / max(1, len(sources)))
    palette = [color, "#2474e5", "#118a7e", "#c58f10", "#8b4cd6", "#4b5563"]
    for idx, source in enumerate(sources):
        offset = (idx - ((len(sources) - 1) / 2.0)) * bar_width
        values = [value_by_key.get((scenario, source), 0.0) for scenario in scenario_names]
        ax.bar([x + offset for x in x_positions], values, width=bar_width, label=source, color=palette[idx % len(palette)])
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.set_ylabel(_field_label(metric))
    ax.set_xticks(x_positions)
    ax.set_xticklabels(scenario_names, rotation=20, ha="right")
    ax.grid(True, axis="y", linestyle="--", alpha=0.28)
    ax.legend(fontsize=8)
    fig.tight_layout()
    png_path = export_dir / "{}.png".format(metric)
    fig.savefig(png_path)
    plt.close(fig)
    return ("{}.png".format(metric), "figure_exports/{}.png".format(metric))


def build_report(sources, out_dir=None):
    rows = _collect_suites(sources)
    if out_dir is None:
        stamp = time.strftime("%Y%m%d_%H%M%S")
        out_dir = LAB_REPORTS_DIR / "lab_compare_{}".format(stamp)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    _write_tsv(out_dir / "lab_overview.tsv", rows, OVERVIEW_FIELDS)
    figure_links = []
    for metric, title, color in METRIC_SPECS:
        figure_links.append(_write_metric_chart(out_dir, rows, metric, title, color))

    source_count = len({row["source"] for row in rows})
    scenario_count = len({row["scenario"] for row in rows})
    run_count = sum(int(row.get("runs", 0) or 0) for row in rows)
    cards = [
        {"label": "Sources", "value": str(source_count), "note": "lab machines or base ports", "tone": "accent"},
        {"label": "Scenarios", "value": str(scenario_count), "note": "scenario groups found", "tone": "accent"},
        {"label": "Runs", "value": str(run_count), "note": "total rows merged from all_runs.tsv", "tone": "accent"},
        {"label": "Output", "value": "Ready", "note": str(out_dir.relative_to(ROOT_DIR)) if str(out_dir).startswith(str(ROOT_DIR)) else str(out_dir), "tone": "good"},
    ]
    sections = [
        _render_links_html("Figure Exports", figure_links + [("lab_overview.tsv", "lab_overview.tsv")]),
        _render_table_html("Lab Comparison Overview", rows, OVERVIEW_FIELDS, "Each row is the latest suite found for a source/scenario pair."),
    ]
    for metric, title, _ in METRIC_SPECS:
        metric_rows = [
            {
                "scenario": row["scenario"],
                "source": row["source"],
                "value": _format_display_value(metric.replace("avg_", ""), row.get(metric, "")) or row.get(metric, ""),
            }
            for row in rows
            if str(row.get(metric, "")).strip()
        ]
        sections.append(_render_table_html(title, metric_rows, ["scenario", "source", "value"]))
    _write_text(
        out_dir / "index.html",
        _html_page(
            "EGESS Lab Comparison",
            "Final suite metrics across lab computers and base ports",
            _render_cards_html(cards),
            "".join(sections),
        ),
    )
    return out_dir


def main():
    parser = argparse.ArgumentParser(description="Compare finished EGESS paper_reports folders across lab machines/base ports.")
    parser.add_argument(
        "--root",
        action="append",
        default=[],
        help="Report root or LABEL=/path/to/paper_reports. Repeat for copied reports from other computers.",
    )
    parser.add_argument("--out-dir", help="Optional output directory for the comparison dashboard.")
    args = parser.parse_args()

    source_args = args.root or [str(ROOT_DIR / "paper_reports")]
    sources = [_parse_source(item) for item in source_args]
    out_dir = build_report(sources, out_dir=Path(args.out_dir).expanduser() if args.out_dir else None)
    print("Lab comparison directory: {}".format(out_dir))


if __name__ == "__main__":
    main()
