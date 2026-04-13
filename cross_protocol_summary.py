#!/usr/bin/env python3
"""Merge EGESS and Check-In suite outputs into one paper-ready comparison page.

This script is designed for the "different computers" workflow:
1. Run EGESS on one machine and Check-In on another.
2. Copy each machine's final `paper_reports/` directory onto one computer.
3. Run this script against those two report roots.
"""

import argparse
import statistics
import time
from pathlib import Path

from paper_eval_runner import (
    _field_label,
    _format_display_value,
    _html_page,
    _matplotlib_pyplot,
    _read_tsv_rows,
    _render_cards_html,
    _render_links_html,
    _render_table_html,
    _scenario_label,
    _write_text,
    _write_tsv,
)


ROOT_DIR = Path(__file__).resolve().parent
COMPARISON_REPORTS_DIR = ROOT_DIR / "comparison_reports"
SCENARIO_ORDER = ["Baseline", "Fire", "Tornado", "Ghost Outage + Noise"]
METRIC_SPECS = [
    ("avg_total_mb", "Overhead (MB)", True, "#ff7a59"),
    ("avg_failures", "Failures / Run", True, "#c73a3a"),
    ("avg_detection_speed_sec", "Detection Latency", True, "#2474e5"),
    ("avg_false_positive_nodes", "False Positives", True, "#8b4cd6"),
    ("avg_false_unavailable_refs", "False Unavailable Refs", True, "#c58f10"),
    ("avg_settle_accuracy_pct", "Settle Accuracy", False, "#118a7e"),
]


def _suite_dirs(root):
    root_path = Path(root)
    if (root_path / "all_runs.tsv").exists():
        return [root_path]
    if not root_path.exists():
        return []
    return sorted([path for path in root_path.iterdir() if path.is_dir() and (path / "all_runs.tsv").exists()])


def _latest_suites(root):
    out = {}
    for suite_dir in _suite_dirs(root):
        rows = _read_tsv_rows(suite_dir / "all_runs.tsv")
        if not rows:
            continue
        row0 = rows[0]
        signature = (str(row0.get("phase_id", "")).strip(), str(row0.get("challenge", "")).strip())
        previous = out.get(signature)
        if previous is None or suite_dir.stat().st_mtime > previous["mtime"]:
            out[signature] = {
                "suite_dir": suite_dir,
                "mtime": suite_dir.stat().st_mtime,
                "rows": rows,
                "summary_by_nodes": _read_tsv_rows(suite_dir / "summary_by_nodes.tsv"),
                "metric_averages": _read_tsv_rows(suite_dir / "metric_averages.tsv"),
            }
    return out


def _avg(rows, field):
    values = []
    for row in rows:
        try:
            values.append(float(row.get(field, "")))
        except Exception:
            continue
    return statistics.mean(values) if values else None


def _avg_failures(rows):
    values = []
    for row in rows:
        try:
            values.append(
                float(row.get("tx_fail_total", 0))
                + float(row.get("tx_timeout_total", 0))
                + float(row.get("tx_conn_error_total", 0))
            )
        except Exception:
            continue
    return statistics.mean(values) if values else None


def _suite_metrics(entry):
    if not entry:
        return None
    rows = entry.get("rows", [])
    summary_by_nodes = entry.get("summary_by_nodes", [])
    node_counts = sorted({int(float(row.get("nodes", 0))) for row in rows if row.get("nodes", "") not in ("", None)})
    durations = sorted({int(float(row.get("duration_sec", 0))) for row in rows if row.get("duration_sec", "") not in ("", None)})
    return {
        "rows": rows,
        "summary_by_nodes": summary_by_nodes,
        "avg_total_mb": _avg(rows, "total_mb"),
        "avg_failures": _avg_failures(rows),
        "avg_detection_speed_sec": _avg(rows, "detection_speed_sec"),
        "avg_false_positive_nodes": _avg(rows, "false_positive_nodes"),
        "avg_false_unavailable_refs": _avg(rows, "false_unavailable_refs"),
        "avg_settle_accuracy_pct": _avg(rows, "settle_accuracy_pct"),
        "setup": "N{} · {}s · {} runs".format(
            "/".join(str(value) for value in node_counts) if node_counts else "n/a",
            "/".join(str(value) for value in durations) if durations else "n/a",
            len(rows),
        ),
        "node_counts": node_counts,
    }


def _winner_callout(egess_value, checkin_value, lower_is_better=True):
    if egess_value is None and checkin_value is None:
        return "Missing"
    if egess_value is None:
        return "Check-In"
    if checkin_value is None:
        return "EGESS"
    if abs(float(egess_value) - float(checkin_value)) <= max(0.01, 0.05 * max(abs(float(egess_value)), abs(float(checkin_value)), 1.0)):
        return "Close"
    if lower_is_better:
        return "EGESS" if float(egess_value) < float(checkin_value) else "Check-In"
    return "EGESS" if float(egess_value) > float(checkin_value) else "Check-In"


def _metric_display(field, value):
    if value is None:
        return "n/a"
    if field == "avg_total_mb":
        return "{:.3f} MB".format(float(value))
    if field == "avg_detection_speed_sec":
        return _format_display_value("detection_speed_sec", value)
    if field == "avg_settle_accuracy_pct":
        return _format_display_value("settle_accuracy_pct", value)
    if field in ("avg_false_positive_nodes", "avg_false_unavailable_refs", "avg_failures"):
        return "{:.2f}".format(float(value))
    return "{:.3f}".format(float(value))


def _scenario_slug(label):
    return str(label).strip().lower().replace(" ", "-").replace("+", "plus")


def _build_overview_rows(egess_suites, checkin_suites):
    signatures = sorted(set(egess_suites.keys()) | set(checkin_suites.keys()), key=lambda item: SCENARIO_ORDER.index(_scenario_label(*item)) if _scenario_label(*item) in SCENARIO_ORDER else 99)
    rows = []
    for signature in signatures:
        label = _scenario_label(*signature)
        egess_metrics = _suite_metrics(egess_suites.get(signature))
        checkin_metrics = _suite_metrics(checkin_suites.get(signature))
        rows.append(
            {
                "scenario": label,
                "egess_setup": egess_metrics.get("setup", "n/a") if egess_metrics else "n/a",
                "checkin_setup": checkin_metrics.get("setup", "n/a") if checkin_metrics else "n/a",
                "bytes_winner": _winner_callout(
                    egess_metrics.get("avg_total_mb") if egess_metrics else None,
                    checkin_metrics.get("avg_total_mb") if checkin_metrics else None,
                    lower_is_better=True,
                ),
                "failures_winner": _winner_callout(
                    egess_metrics.get("avg_failures") if egess_metrics else None,
                    checkin_metrics.get("avg_failures") if checkin_metrics else None,
                    lower_is_better=True,
                ),
                "detection_latency_winner": _winner_callout(
                    egess_metrics.get("avg_detection_speed_sec") if egess_metrics else None,
                    checkin_metrics.get("avg_detection_speed_sec") if checkin_metrics else None,
                    lower_is_better=True,
                ),
                "false_positive_winner": _winner_callout(
                    egess_metrics.get("avg_false_positive_nodes") if egess_metrics else None,
                    checkin_metrics.get("avg_false_positive_nodes") if checkin_metrics else None,
                    lower_is_better=True,
                ),
                "false_unavailable_winner": _winner_callout(
                    egess_metrics.get("avg_false_unavailable_refs") if egess_metrics else None,
                    checkin_metrics.get("avg_false_unavailable_refs") if checkin_metrics else None,
                    lower_is_better=True,
                ),
                "accuracy_winner": _winner_callout(
                    egess_metrics.get("avg_settle_accuracy_pct") if egess_metrics else None,
                    checkin_metrics.get("avg_settle_accuracy_pct") if checkin_metrics else None,
                    lower_is_better=False,
                ),
            }
        )
    return rows


def _summary_by_nodes_index(summary_rows):
    out = {}
    for row in summary_rows or []:
        try:
            out[int(float(row.get("nodes", 0)))] = row
        except Exception:
            continue
    return out


def _combined_metric_rows(egess_entry, checkin_entry):
    egess_by_nodes = _summary_by_nodes_index(egess_entry.get("summary_by_nodes") if egess_entry else [])
    checkin_by_nodes = _summary_by_nodes_index(checkin_entry.get("summary_by_nodes") if checkin_entry else [])
    node_counts = sorted(set(egess_by_nodes.keys()) | set(checkin_by_nodes.keys()))
    rows = []
    for field, label, lower_is_better, _color in METRIC_SPECS:
        row = {"metric": label}
        for node_count in node_counts:
            egess_value = egess_by_nodes.get(node_count, {}).get(field, "")
            checkin_value = checkin_by_nodes.get(node_count, {}).get(field, "")
            row["egess_n{}".format(node_count)] = egess_value
            row["checkin_n{}".format(node_count)] = checkin_value
            try:
                egess_num = float(egess_value)
                checkin_num = float(checkin_value)
                delta = egess_num - checkin_num
                if field == "avg_settle_accuracy_pct":
                    row["delta_n{}".format(node_count)] = "{:+.1f}%".format(delta)
                elif field == "avg_total_mb":
                    row["delta_n{}".format(node_count)] = "{:+.3f} MB".format(delta)
                else:
                    row["delta_n{}".format(node_count)] = "{:+.2f}".format(delta)
            except Exception:
                row["delta_n{}".format(node_count)] = "n/a"
            row["winner_n{}".format(node_count)] = _winner_callout(
                float(egess_value) if str(egess_value).strip() else None,
                float(checkin_value) if str(checkin_value).strip() else None,
                lower_is_better=lower_is_better,
            )
        rows.append(row)
    return node_counts, rows


def _render_combined_tables(signatures, egess_suites, checkin_suites, out_dir):
    buttons = []
    panels = []
    combined_files = []
    for idx, signature in enumerate(signatures):
        scenario_label = _scenario_label(*signature)
        slug = _scenario_slug(scenario_label)
        buttons.append('<button type="button" class="scenario-tab {}" data-compare-tab="{}">{}</button>'.format("active" if idx == 0 else "", slug, scenario_label))
        node_counts, metric_rows = _combined_metric_rows(egess_suites.get(signature), checkin_suites.get(signature))
        fields = ["metric"]
        for node_count in node_counts:
            fields.extend(
                [
                    "egess_n{}".format(node_count),
                    "checkin_n{}".format(node_count),
                    "delta_n{}".format(node_count),
                    "winner_n{}".format(node_count),
                ]
            )
        pretty_rows = []
        for row in metric_rows:
            pretty = {"metric": row["metric"]}
            for node_count in node_counts:
                pretty["egess_n{}".format(node_count)] = _metric_display(_field_from_label(row["metric"]), row.get("egess_n{}".format(node_count))) if str(row.get("egess_n{}".format(node_count), "")).strip() else "n/a"
                pretty["checkin_n{}".format(node_count)] = _metric_display(_field_from_label(row["metric"]), row.get("checkin_n{}".format(node_count))) if str(row.get("checkin_n{}".format(node_count), "")).strip() else "n/a"
                pretty["delta_n{}".format(node_count)] = row.get("delta_n{}".format(node_count), "n/a")
                pretty["winner_n{}".format(node_count)] = row.get("winner_n{}".format(node_count), "Missing")
            pretty_rows.append(pretty)
        combined_path = out_dir / "combined_{}.tsv".format(slug)
        _write_tsv(combined_path, pretty_rows, fields)
        combined_files.append(("combined_{}.tsv".format(slug), combined_path.name))
        panels.append(
            '<div class="nodecount-panel {}" data-compare-panel="{}">{}</div>'.format(
                "active" if idx == 0 else "",
                slug,
                _render_table_html(
                    "{} Node Count Comparison".format(scenario_label),
                    pretty_rows,
                    fields,
                    "Columns pair EGESS and Check-In side by side so you can read 49, 64, and 81 at a glance.",
                ),
            )
        )
    script = """<script>
(function () {
  const buttons = Array.from(document.querySelectorAll('[data-compare-tab]'));
  const panels = Array.from(document.querySelectorAll('[data-compare-panel]'));
  if (!buttons.length) return;
  function activate(tab) {
    buttons.forEach((button) => button.classList.toggle('active', button.getAttribute('data-compare-tab') === tab));
    panels.forEach((panel) => panel.classList.toggle('active', panel.getAttribute('data-compare-panel') === tab));
  }
  buttons.forEach((button) => button.addEventListener('click', () => activate(button.getAttribute('data-compare-tab') || '')));
})();
</script>"""
    section = """<section class="panel">
<div class="panel-head">
  <h2>Combined Average Table Across Protocols And Node Counts</h2>
  <p class="section-note">Each scenario gets its own tab. Within a tab, EGESS and Check-In are laid out side by side for 49, 64, and 81 nodes with signed deltas and winner callouts.</p>
</div>
<div class="scenario-tab-row">{buttons}</div>
{panels}
</section>""".format(buttons="".join(buttons), panels="".join(panels))
    return section, script, combined_files


def _field_from_label(label):
    mapping = {metric_label: field for field, metric_label, _lower_is_better, _color in METRIC_SPECS}
    return mapping.get(label, "")


def _write_figure_exports(out_dir, signatures, egess_suites, checkin_suites):
    export_dir = out_dir / "figure_exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    links = []
    created = []
    for signature in signatures:
        scenario_label = _scenario_label(*signature)
        slug = _scenario_slug(scenario_label)
        node_counts, _metric_rows = _combined_metric_rows(egess_suites.get(signature), checkin_suites.get(signature))
        egess_by_nodes = _summary_by_nodes_index((egess_suites.get(signature) or {}).get("summary_by_nodes", []))
        checkin_by_nodes = _summary_by_nodes_index((checkin_suites.get(signature) or {}).get("summary_by_nodes", []))
        for field, metric_label, _lower_is_better, color in METRIC_SPECS:
            tsv_rows = []
            for node_count in node_counts:
                tsv_rows.append(
                    {
                        "nodes": node_count,
                        "egess": egess_by_nodes.get(node_count, {}).get(field, ""),
                        "checkin": checkin_by_nodes.get(node_count, {}).get(field, ""),
                    }
                )
            tsv_path = export_dir / "{}_{}.tsv".format(slug, field)
            _write_tsv(tsv_path, tsv_rows, ["nodes", "egess", "checkin"])
            created.append(tsv_path.name)
            links.append((tsv_path.name, "figure_exports/{}".format(tsv_path.name)))
            valid_rows = [row for row in tsv_rows if str(row.get("egess", "")).strip() or str(row.get("checkin", "")).strip()]
            if not valid_rows:
                continue
            plt = _matplotlib_pyplot()
            fig, ax = plt.subplots(figsize=(7.6, 4.6), dpi=180)
            xs = [int(row["nodes"]) for row in valid_rows]
            egess_vals = [float(row["egess"]) if str(row.get("egess", "")).strip() else None for row in valid_rows]
            checkin_vals = [float(row["checkin"]) if str(row.get("checkin", "")).strip() else None for row in valid_rows]
            ax.plot(xs, egess_vals, marker="o", linewidth=2.3, color=color, label="EGESS")
            ax.plot(xs, checkin_vals, marker="s", linewidth=2.3, color="#374151", label="Check-In")
            ax.set_title("{} · {}".format(scenario_label, metric_label), fontsize=12, fontweight="bold")
            ax.set_xlabel("Node Count")
            ax.set_ylabel(metric_label)
            ax.grid(True, axis="y", linestyle="--", alpha=0.28)
            ax.legend()
            png_path = export_dir / "{}_{}.png".format(slug, field)
            fig.tight_layout()
            fig.savefig(png_path)
            plt.close(fig)
            created.append(png_path.name)
            links.append((png_path.name, "figure_exports/{}".format(png_path.name)))
    readme_path = export_dir / "README.md"
    _write_text(
        readme_path,
        "\n".join(["# Cross-Protocol Figure Exports", ""] + ["- {}".format(item) for item in created]) + "\n",
    )
    links.insert(0, ("figure_exports/README.md", "figure_exports/README.md"))
    return links


def _render_overview_section(rows):
    fields = [
        "scenario",
        "egess_setup",
        "checkin_setup",
        "bytes_winner",
        "failures_winner",
        "detection_latency_winner",
        "false_positive_winner",
        "false_unavailable_winner",
        "accuracy_winner",
    ]
    return _render_table_html(
        "Cross-Protocol Final Summary",
        rows,
        fields,
        "This page compares Baseline, Fire, Tornado, and Stress directly. Winner callouts mark which protocol looked stronger per metric, with Close used for near-ties.",
    )


def main():
    parser = argparse.ArgumentParser(description="Merge EGESS and Check-In paper reports into one cross-protocol summary.")
    parser.add_argument("--egess-root", default=str(ROOT_DIR / "paper_reports"))
    parser.add_argument("--checkin-root", default=str(ROOT_DIR / "external" / "checkin-egess-eval" / "paper_reports"))
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    egess_suites = _latest_suites(args.egess_root)
    checkin_suites = _latest_suites(args.checkin_root)
    signatures = sorted(set(egess_suites.keys()) | set(checkin_suites.keys()), key=lambda item: SCENARIO_ORDER.index(_scenario_label(*item)) if _scenario_label(*item) in SCENARIO_ORDER else 99)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out) if args.out else (COMPARISON_REPORTS_DIR / "cross_protocol_{}".format(timestamp))
    out_dir.mkdir(parents=True, exist_ok=True)

    overview_rows = _build_overview_rows(egess_suites, checkin_suites)
    combined_html, combined_script, combined_files = _render_combined_tables(signatures, egess_suites, checkin_suites, out_dir)
    figure_links = _write_figure_exports(out_dir, signatures, egess_suites, checkin_suites)

    overview_path = out_dir / "cross_protocol_overview.tsv"
    _write_tsv(
        overview_path,
        overview_rows,
        [
            "scenario",
            "egess_setup",
            "checkin_setup",
            "bytes_winner",
            "failures_winner",
            "detection_latency_winner",
            "false_positive_winner",
            "false_unavailable_winner",
            "accuracy_winner",
        ],
    )

    cards = [
        {"label": "Scenarios", "value": str(len(overview_rows)), "note": "Baseline, Fire, Tornado, Stress", "tone": "accent"},
        {"label": "EGESS Suites", "value": str(len(egess_suites)), "note": str(Path(args.egess_root)), "tone": "accent"},
        {"label": "Check-In Suites", "value": str(len(checkin_suites)), "note": str(Path(args.checkin_root)), "tone": "accent"},
        {"label": "Figure Exports", "value": "Ready", "note": "PNG plus TSV for each comparison figure", "tone": "good"},
    ]

    sections = [
        _render_overview_section(overview_rows),
        combined_html,
        _render_links_html(
            "Raw Output Files",
            [("cross_protocol_overview.tsv", "cross_protocol_overview.tsv")] + combined_files + figure_links,
        ),
    ]
    html = _html_page(
        "Cross-Protocol Paper Summary",
        "Merged from separate EGESS and Check-In report roots. This is the post-processing page you can build after both teams finish running on different computers.",
        _render_cards_html(cards),
        "".join(sections),
        script_html=combined_script,
    )
    _write_text(out_dir / "index.html", html)
    _write_text(
        out_dir / "README.md",
        "\n".join(
            [
                "# Cross-Protocol Paper Summary",
                "",
                "- EGESS root: `{}`".format(args.egess_root),
                "- Check-In root: `{}`".format(args.checkin_root),
                "- Overview table: `cross_protocol_overview.tsv`",
                "- Dashboard: `index.html`",
                "- Figure exports: `figure_exports/README.md`",
            ]
        )
        + "\n",
    )
    print(str(out_dir))


if __name__ == "__main__":
    main()
