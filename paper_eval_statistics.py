#!/usr/bin/env python3
"""Post-process paper eval outputs with statistical tests and graph exports.

This script is meant to run after EGESS and Check-In finish collecting data,
even if they were collected on different computers. It reads each protocol's
`paper_reports/` folder and writes a paper-ready statistics report.
"""

from __future__ import annotations

import argparse
import math
import statistics
import time
from collections import defaultdict
from pathlib import Path
from statistics import NormalDist

from paper_eval_runner import (
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
STAT_REPORTS_DIR = ROOT_DIR / "statistics_reports"

RUN_METRICS = [
    "detection_speed_sec",
    "total_mb",
    "total_bytes",
    "tx_fail_total",
    "tx_timeout_total",
    "tx_conn_error_total",
    "false_positive_nodes",
    "false_unavailable_refs",
    "settle_accuracy_pct",
    "accepted_messages_total",
    "pull_rx_total",
    "push_rx_total",
    "pull_tx_total",
    "push_tx_total",
]

WATCH_METRICS = [
    "total_mb",
    "total_bytes",
    "accepted_messages",
    "pull_rx",
    "push_rx",
    "pull_tx",
    "push_tx",
    "current_missing_count",
]

KEY_RUN_METRICS = [
    "detection_speed_sec",
    "total_mb",
    "tx_fail_total",
    "false_positive_nodes",
    "false_unavailable_refs",
    "settle_accuracy_pct",
]

LOWER_IS_BETTER = {
    "detection_speed_sec": True,
    "total_mb": True,
    "total_bytes": True,
    "tx_fail_total": True,
    "tx_timeout_total": True,
    "tx_conn_error_total": True,
    "false_positive_nodes": True,
    "false_unavailable_refs": True,
    "current_missing_count": True,
    "accepted_messages": False,
    "accepted_messages_total": False,
    "pull_rx_total": True,
    "push_rx_total": True,
    "pull_tx_total": True,
    "push_tx_total": True,
    "pull_rx": True,
    "push_rx": True,
    "pull_tx": True,
    "push_tx": True,
    "settle_accuracy_pct": False,
}

METRIC_LABELS = {
    "detection_speed_sec": "Detection Latency (s)",
    "total_mb": "Overhead (MB)",
    "total_bytes": "Overhead (bytes)",
    "tx_fail_total": "TX Failures",
    "tx_timeout_total": "TX Timeouts",
    "tx_conn_error_total": "Connection Errors",
    "false_positive_nodes": "False Positive Nodes",
    "false_unavailable_refs": "False Unavailable References",
    "settle_accuracy_pct": "Settle Accuracy (%)",
    "accepted_messages": "Accepted Messages",
    "accepted_messages_total": "Accepted Messages",
    "pull_rx": "Pull RX",
    "pull_rx_total": "Pull RX",
    "push_rx": "Push RX",
    "push_rx_total": "Push RX",
    "pull_tx": "Pull TX",
    "pull_tx_total": "Pull TX",
    "push_tx": "Push TX",
    "push_tx_total": "Push TX",
    "current_missing_count": "Missing Neighbor Count",
}

STATS_FIELDS = [
    "source",
    "protocol",
    "scenario",
    "nodes",
    "duration_sec",
    "view",
    "metric",
    "n",
    "mean",
    "sample_std",
    "standard_error",
    "ci95_low",
    "ci95_high",
    "min",
    "p50",
    "p90",
    "p95",
    "p99",
    "max",
]

PAIRED_FIELDS = [
    "scenario",
    "nodes",
    "duration_sec",
    "metric",
    "pairs",
    "egess_mean",
    "checkin_mean",
    "mean_difference_egess_minus_checkin",
    "sample_std_difference",
    "ci95_low_difference",
    "ci95_high_difference",
    "t_statistic",
    "p_value_approx",
    "significant_at_95",
    "winner",
]


def _to_float(value):
    if value in ("", None, "None", "nan", "NaN"):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _to_int(value, fallback=0):
    number = _to_float(value)
    return int(number) if number is not None else int(fallback)


def _suite_dirs(root):
    root = Path(root)
    if (root / "all_runs.tsv").exists():
        return [root]
    if not root.exists():
        return []
    return sorted(path for path in root.iterdir() if path.is_dir() and (path / "all_runs.tsv").exists())


def _row_scenario(row):
    return _scenario_label(str(row.get("phase_id", "")), str(row.get("challenge", "")))


def _latest_grouped_rows(report_root, protocol_label, filename):
    """Use the newest suite for each scenario/node/duration group.

    This prevents old smoke runs from mixing with fresh full runs while still
    allowing teams to collect 49, 64, and 89 in separate campaigns.
    """
    grouped = {}
    for suite_dir in _suite_dirs(report_root):
        path = suite_dir / filename
        if not path.exists():
            continue
        rows = _read_tsv_rows(path)
        by_key = defaultdict(list)
        for row in rows:
            protocol = str(row.get("protocol", protocol_label) or protocol_label).strip().lower() or protocol_label
            scenario = _row_scenario(row)
            key = (
                protocol,
                scenario,
                str(row.get("phase_id", "")),
                str(row.get("challenge", "")),
                _to_int(row.get("nodes", 0), 0),
                _to_int(row.get("duration_sec", 0), 0),
            )
            normalized = dict(row)
            normalized["protocol"] = protocol_label
            normalized["scenario"] = scenario
            normalized["_suite_dir"] = str(suite_dir)
            by_key[key].append(normalized)
        for key, key_rows in by_key.items():
            previous = grouped.get(key)
            if previous is None or suite_dir.stat().st_mtime >= previous["mtime"]:
                grouped[key] = {"mtime": suite_dir.stat().st_mtime, "rows": key_rows}
    out = []
    for payload in grouped.values():
        out.extend(payload["rows"])
    return out


def _load_protocol_rows(egess_root, checkin_root):
    return {
        "egess": {
            "runs": _latest_grouped_rows(egess_root, "egess", "all_runs.tsv"),
            "watch": _latest_grouped_rows(egess_root, "egess", "all_watch_nodes.tsv"),
        },
        "checkin": {
            "runs": _latest_grouped_rows(checkin_root, "checkin", "all_runs.tsv"),
            "watch": _latest_grouped_rows(checkin_root, "checkin", "all_watch_nodes.tsv"),
        },
    }


def _percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * (float(pct) / 100.0)
    low = int(math.floor(pos))
    high = int(math.ceil(pos))
    if low == high:
        return ordered[low]
    weight = pos - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def _t_critical_95(n):
    df = max(1, int(n) - 1)
    table = {
        1: 12.706,
        2: 4.303,
        3: 3.182,
        4: 2.776,
        5: 2.571,
        6: 2.447,
        7: 2.365,
        8: 2.306,
        9: 2.262,
        10: 2.228,
        11: 2.201,
        12: 2.179,
        13: 2.160,
        14: 2.145,
        15: 2.131,
        16: 2.120,
        17: 2.110,
        18: 2.101,
        19: 2.093,
        20: 2.086,
        21: 2.080,
        22: 2.074,
        23: 2.069,
        24: 2.064,
        25: 2.060,
        26: 2.056,
        27: 2.052,
        28: 2.048,
        29: 2.045,
        30: 2.042,
    }
    if df in table:
        return table[df]
    if df <= 40:
        return 2.021
    if df <= 60:
        return 2.000
    if df <= 120:
        return 1.980
    return 1.960


def _stats(values):
    values = [float(value) for value in values if value is not None]
    n = len(values)
    if n == 0:
        return None
    mean = statistics.mean(values)
    sample_std = statistics.stdev(values) if n > 1 else 0.0
    standard_error = sample_std / math.sqrt(n) if n > 1 else 0.0
    margin = _t_critical_95(n) * standard_error if n > 1 else 0.0
    return {
        "n": n,
        "mean": mean,
        "sample_std": sample_std,
        "standard_error": standard_error,
        "ci95_low": mean - margin,
        "ci95_high": mean + margin,
        "min": min(values),
        "p50": _percentile(values, 50),
        "p90": _percentile(values, 90),
        "p95": _percentile(values, 95),
        "p99": _percentile(values, 99),
        "max": max(values),
    }


def _fmt(value):
    if value is None:
        return ""
    try:
        return "{:.6g}".format(float(value))
    except Exception:
        return str(value)


def _stats_rows(protocol_rows):
    grouped = defaultdict(list)
    for protocol, payload in protocol_rows.items():
        for row in payload["runs"]:
            for metric in RUN_METRICS:
                value = _to_float(row.get(metric, ""))
                if value is None:
                    continue
                key = ("run", protocol, row["scenario"], _to_int(row.get("nodes", 0), 0), _to_int(row.get("duration_sec", 0), 0), "ALL", metric)
                grouped[key].append(value)
        for row in payload["watch"]:
            for metric in WATCH_METRICS:
                value = _to_float(row.get(metric, ""))
                if value is None:
                    continue
                view = str(row.get("view", "WATCH")).upper() or "WATCH"
                key = ("watch_node", protocol, row["scenario"], _to_int(row.get("nodes", 0), 0), _to_int(row.get("duration_sec", 0), 0), view, metric)
                grouped[key].append(value)

    rows = []
    for key, values in sorted(grouped.items()):
        source, protocol, scenario, nodes, duration, view, metric = key
        stats = _stats(values)
        if not stats:
            continue
        row = {
            "source": source,
            "protocol": protocol,
            "scenario": scenario,
            "nodes": nodes,
            "duration_sec": duration,
            "view": view,
            "metric": metric,
        }
        row.update({field: _fmt(stats[field]) for field in STATS_FIELDS if field in stats})
        rows.append(row)
    return rows


def _overhead_percentile_rows(stats_rows):
    rows = []
    for row in stats_rows:
        if row.get("metric") not in ("total_mb", "total_bytes"):
            continue
        rows.append(
            {
                "scope": row.get("source", ""),
                "protocol": row.get("protocol", ""),
                "scenario": row.get("scenario", ""),
                "nodes": row.get("nodes", ""),
                "duration_sec": row.get("duration_sec", ""),
                "view": row.get("view", ""),
                "metric": row.get("metric", ""),
                "n": row.get("n", ""),
                "p50": row.get("p50", ""),
                "p90": row.get("p90", ""),
                "p95": row.get("p95", ""),
                "p99": row.get("p99", ""),
                "max": row.get("max", ""),
            }
        )
    return rows


def _paired_rows(protocol_rows):
    egess = protocol_rows["egess"]["runs"]
    checkin = protocol_rows["checkin"]["runs"]
    egess_index = {}
    checkin_index = {}
    for row in egess:
        key = (row["scenario"], _to_int(row.get("nodes", 0), 0), _to_int(row.get("duration_sec", 0), 0), _to_int(row.get("run_index", 0), 0), _to_int(row.get("seed", 0), 0))
        egess_index[key] = row
    for row in checkin:
        key = (row["scenario"], _to_int(row.get("nodes", 0), 0), _to_int(row.get("duration_sec", 0), 0), _to_int(row.get("run_index", 0), 0), _to_int(row.get("seed", 0), 0))
        checkin_index[key] = row

    grouped = defaultdict(list)
    for key in sorted(set(egess_index.keys()) & set(checkin_index.keys())):
        scenario, nodes, duration, _run_index, _seed = key
        egess_row = egess_index[key]
        checkin_row = checkin_index[key]
        for metric in KEY_RUN_METRICS:
            egess_value = _to_float(egess_row.get(metric, ""))
            checkin_value = _to_float(checkin_row.get(metric, ""))
            if egess_value is None or checkin_value is None:
                continue
            grouped[(scenario, nodes, duration, metric)].append((egess_value, checkin_value, egess_value - checkin_value))

    rows = []
    normal = NormalDist()
    for (scenario, nodes, duration, metric), pairs in sorted(grouped.items()):
        diffs = [item[2] for item in pairs]
        diff_stats = _stats(diffs)
        if not diff_stats:
            continue
        n = len(diffs)
        mean_diff = diff_stats["mean"]
        std_diff = diff_stats["sample_std"]
        se = diff_stats["standard_error"]
        t_stat = mean_diff / se if se > 0 else 0.0
        p_approx = 2.0 * (1.0 - normal.cdf(abs(t_stat))) if n > 1 else 1.0
        significant = "Yes" if n > 1 and abs(t_stat) >= _t_critical_95(n) else "No"
        egess_mean = statistics.mean(item[0] for item in pairs)
        checkin_mean = statistics.mean(item[1] for item in pairs)
        if abs(mean_diff) <= max(0.01, 0.05 * max(abs(egess_mean), abs(checkin_mean), 1.0)):
            winner = "Close"
        elif LOWER_IS_BETTER.get(metric, True):
            winner = "EGESS" if egess_mean < checkin_mean else "Check-In"
        else:
            winner = "EGESS" if egess_mean > checkin_mean else "Check-In"
        rows.append(
            {
                "scenario": scenario,
                "nodes": nodes,
                "duration_sec": duration,
                "metric": metric,
                "pairs": n,
                "egess_mean": _fmt(egess_mean),
                "checkin_mean": _fmt(checkin_mean),
                "mean_difference_egess_minus_checkin": _fmt(mean_diff),
                "sample_std_difference": _fmt(std_diff),
                "ci95_low_difference": _fmt(diff_stats["ci95_low"]),
                "ci95_high_difference": _fmt(diff_stats["ci95_high"]),
                "t_statistic": _fmt(t_stat),
                "p_value_approx": _fmt(p_approx),
                "significant_at_95": significant,
                "winner": winner,
            }
        )
    return rows


def _raw_metric_values(protocol_rows, source, metric):
    rows = []
    for protocol, payload in protocol_rows.items():
        source_rows = payload["runs"] if source == "run" else payload["watch"]
        for row in source_rows:
            value = _to_float(row.get(metric, ""))
            if value is None:
                continue
            rows.append(
                {
                    "protocol": protocol,
                    "scenario": row["scenario"],
                    "nodes": _to_int(row.get("nodes", 0), 0),
                    "duration_sec": _to_int(row.get("duration_sec", 0), 0),
                    "view": str(row.get("view", "ALL")).upper() if source == "watch" else "ALL",
                    "metric": metric,
                    "value": value,
                }
            )
    return rows


def _boxplot_rows(protocol_rows):
    out = []
    for source, metrics in (("run", KEY_RUN_METRICS), ("watch", ("total_mb", "accepted_messages", "pull_rx", "push_rx"))):
        grouped = defaultdict(list)
        for metric in metrics:
            for row in _raw_metric_values(protocol_rows, source, metric):
                key = (source, row["protocol"], row["scenario"], row["nodes"], row["duration_sec"], row["view"], metric)
                grouped[key].append(row["value"])
        for key, values in sorted(grouped.items()):
            source, protocol, scenario, nodes, duration, view, metric = key
            q1 = _percentile(values, 25)
            q2 = _percentile(values, 50)
            q3 = _percentile(values, 75)
            iqr = float(q3 - q1) if q1 is not None and q3 is not None else 0.0
            low_fence = q1 - 1.5 * iqr
            high_fence = q3 + 1.5 * iqr
            non_outliers = [value for value in values if low_fence <= value <= high_fence]
            out.append(
                {
                    "source": source,
                    "protocol": protocol,
                    "scenario": scenario,
                    "nodes": nodes,
                    "duration_sec": duration,
                    "view": view,
                    "metric": metric,
                    "n": len(values),
                    "min": _fmt(min(values)),
                    "q1": _fmt(q1),
                    "median": _fmt(q2),
                    "q3": _fmt(q3),
                    "max": _fmt(max(values)),
                    "lower_whisker": _fmt(min(non_outliers) if non_outliers else min(values)),
                    "upper_whisker": _fmt(max(non_outliers) if non_outliers else max(values)),
                    "outlier_count": len(values) - len(non_outliers),
                }
            )
    return out


def _cdf_rows(protocol_rows):
    out = []
    for metric in ("detection_speed_sec", "total_mb", "tx_fail_total"):
        grouped = defaultdict(list)
        for row in _raw_metric_values(protocol_rows, "run", metric):
            key = (row["protocol"], row["scenario"], row["nodes"], row["duration_sec"], metric)
            grouped[key].append(row["value"])
        for key, values in sorted(grouped.items()):
            protocol, scenario, nodes, duration, metric = key
            ordered = sorted(values)
            n = len(ordered)
            for idx, value in enumerate(ordered, start=1):
                out.append(
                    {
                        "protocol": protocol,
                        "scenario": scenario,
                        "nodes": nodes,
                        "duration_sec": duration,
                        "metric": metric,
                        "value": _fmt(value),
                        "cdf": _fmt(idx / float(n)),
                    }
                )
    return out


def _histogram_rows(protocol_rows, bins=10):
    out = []
    for metric in ("detection_speed_sec", "total_mb", "tx_fail_total"):
        grouped = defaultdict(list)
        for row in _raw_metric_values(protocol_rows, "run", metric):
            key = (row["protocol"], row["scenario"], row["nodes"], row["duration_sec"], metric)
            grouped[key].append(row["value"])
        for key, values in sorted(grouped.items()):
            protocol, scenario, nodes, duration, metric = key
            low = min(values)
            high = max(values)
            if math.isclose(low, high):
                out.append({"protocol": protocol, "scenario": scenario, "nodes": nodes, "duration_sec": duration, "metric": metric, "bin_low": _fmt(low), "bin_high": _fmt(high), "count": len(values)})
                continue
            width = (high - low) / float(bins)
            counts = [0 for _ in range(bins)]
            for value in values:
                idx = min(bins - 1, int((value - low) / width))
                counts[idx] += 1
            for idx, count in enumerate(counts):
                out.append(
                    {
                        "protocol": protocol,
                        "scenario": scenario,
                        "nodes": nodes,
                        "duration_sec": duration,
                        "metric": metric,
                        "bin_low": _fmt(low + idx * width),
                        "bin_high": _fmt(low + (idx + 1) * width),
                        "count": count,
                    }
                )
    return out


def _metric_label(metric):
    metric = str(metric or "")
    return METRIC_LABELS.get(metric, metric.replace("_", " ").title())


def _rows_with_metric_labels(rows):
    labeled = []
    for row in rows:
        copy = dict(row)
        if "metric" in copy:
            copy["metric"] = _metric_label(copy.get("metric"))
        labeled.append(copy)
    return labeled


def _write_mean_ci_figures(out_dir, stats_rows):
    fig_dir = out_dir / "figure_exports"
    fig_dir.mkdir(parents=True, exist_ok=True)
    created = []
    try:
        plt = _matplotlib_pyplot()
    except Exception:
        return created

    grouped = defaultdict(list)
    for row in stats_rows:
        if row.get("source") != "run" or row.get("view") != "ALL" or row.get("metric") not in KEY_RUN_METRICS:
            continue
        grouped[(row["scenario"], row["metric"])].append(row)

    for (scenario, metric), rows in sorted(grouped.items()):
        metric_label = _metric_label(metric)
        nodes = sorted({_to_int(row.get("nodes", 0), 0) for row in rows})
        if not nodes:
            continue
        fig, ax = plt.subplots(figsize=(7.8, 4.6), dpi=180)
        for protocol, color, marker in (("egess", "#2474e5", "o"), ("checkin", "#374151", "s")):
            protocol_rows = {(_to_int(row.get("nodes", 0), 0)): row for row in rows if row.get("protocol") == protocol}
            xs = [node for node in nodes if node in protocol_rows]
            if not xs:
                continue
            means = [float(protocol_rows[node]["mean"]) for node in xs]
            lows = [float(protocol_rows[node]["ci95_low"]) for node in xs]
            highs = [float(protocol_rows[node]["ci95_high"]) for node in xs]
            lower_err = [mean - low for mean, low in zip(means, lows)]
            upper_err = [high - mean for mean, high in zip(means, highs)]
            ax.errorbar(xs, means, yerr=[lower_err, upper_err], marker=marker, linewidth=2.2, capsize=4, color=color, label=protocol.upper())
        ax.set_title("{} · {} mean with 95% CI".format(scenario, metric_label), fontsize=12, fontweight="bold")
        ax.set_xlabel("Node Count")
        ax.set_ylabel(metric_label)
        ax.grid(True, axis="y", linestyle="--", alpha=0.28)
        ax.legend()
        png_name = "{}_{}_mean_ci.png".format(scenario.lower().replace(" ", "_").replace("+", "plus"), metric)
        fig.tight_layout()
        fig.savefig(fig_dir / png_name)
        plt.close(fig)
        created.append(png_name)
    return created


def _write_raw_exports(out_dir, stats_rows, paired_rows, overhead_rows, boxplot_rows, cdf_rows, histogram_rows):
    _write_tsv(out_dir / "metric_statistics.tsv", stats_rows, STATS_FIELDS)
    _write_tsv(out_dir / "paired_t_tests.tsv", paired_rows, PAIRED_FIELDS)
    _write_tsv(out_dir / "overhead_percentiles.tsv", overhead_rows, ["scope", "protocol", "scenario", "nodes", "duration_sec", "view", "metric", "n", "p50", "p90", "p95", "p99", "max"])
    _write_tsv(out_dir / "boxplot_data.tsv", boxplot_rows, ["source", "protocol", "scenario", "nodes", "duration_sec", "view", "metric", "n", "min", "q1", "median", "q3", "max", "lower_whisker", "upper_whisker", "outlier_count"])
    _write_tsv(out_dir / "cdf_points.tsv", cdf_rows, ["protocol", "scenario", "nodes", "duration_sec", "metric", "value", "cdf"])
    _write_tsv(out_dir / "histogram_bins.tsv", histogram_rows, ["protocol", "scenario", "nodes", "duration_sec", "metric", "bin_low", "bin_high", "count"])


def _render_html(out_dir, stats_rows, paired_rows, overhead_rows, figure_names, egess_root, checkin_root):
    run_stat_rows = [row for row in stats_rows if row.get("source") == "run" and row.get("metric") in KEY_RUN_METRICS]
    watch_stat_rows = [row for row in stats_rows if row.get("source") == "watch_node" and row.get("metric") in ("total_mb", "accepted_messages", "pull_rx", "push_rx")]
    cards = [
        {"label": "Run Metric Groups", "value": str(len(run_stat_rows)), "note": "means, std dev, CI, percentiles", "tone": "accent"},
        {"label": "Paired Tests", "value": str(len(paired_rows)), "note": "matched by scenario, node count, run, seed", "tone": "accent"},
        {"label": "Overhead Percentiles", "value": str(len(overhead_rows)), "note": "p50, p90, p95, p99", "tone": "accent"},
        {"label": "Figures", "value": str(len(figure_names)), "note": "PNG mean-CI exports", "tone": "good"},
    ]
    links = [
        ("metric_statistics.tsv", "metric_statistics.tsv"),
        ("paired_t_tests.tsv", "paired_t_tests.tsv"),
        ("overhead_percentiles.tsv", "overhead_percentiles.tsv"),
        ("boxplot_data.tsv", "boxplot_data.tsv"),
        ("cdf_points.tsv", "cdf_points.tsv"),
        ("histogram_bins.tsv", "histogram_bins.tsv"),
    ]
    links.extend(("figure_exports/{}".format(name), "figure_exports/{}".format(name)) for name in figure_names[:24])
    sections = [
        """
<section class="panel">
  <h2>Statistical Method</h2>
  <p class="section-note">This post-processing report estimates population means with 95% confidence intervals, summarizes overhead percentiles, exports graph-ready data for histograms/boxplots/CDFs, and runs paired t-tests when EGESS and Check-In have matching scenario, node count, run index, and seed.</p>
  <div class="guide-grid">
    <div class="guide-card"><h3>Confidence Interval</h3><p>Mean ± t-critical × standard error. Use it to report likely population mean ranges for detection latency, throughput, overhead, and node load.</p></div>
    <div class="guide-card"><h3>Percentiles</h3><p>p50/p90/p95/p99 show typical and tail behavior. This is best for overhead and detection latency because averages can hide slow or expensive runs.</p></div>
    <div class="guide-card"><h3>Graphs</h3><p>Boxplot, histogram, and CDF TSVs are generated so the team can visualize skew, outliers, and distribution differences.</p></div>
    <div class="guide-card"><h3>Paired T-Test</h3><p>Pairs use the same scenario, node count, duration, run index, and seed. Significant means the observed difference is unlikely to be random at the 95% level.</p></div>
  </div>
</section>
""",
        _render_table_html("Run Metrics With 95% Confidence Intervals", _rows_with_metric_labels(run_stat_rows[:160]), STATS_FIELDS, "Grouped by protocol, scenario, node count, duration, and metric."),
        _render_table_html("Watched Node Load Statistics", _rows_with_metric_labels(watch_stat_rows[:160]), STATS_FIELDS, "Useful for single-node load and per-node overhead claims."),
        _render_table_html("Overhead Percentiles", _rows_with_metric_labels(overhead_rows[:160]), ["scope", "protocol", "scenario", "nodes", "duration_sec", "view", "metric", "n", "p50", "p90", "p95", "p99", "max"], "p95 is the clean paper number for tail overhead."),
        _render_table_html("Paired T-Tests", paired_rows[:160], PAIRED_FIELDS, "Only appears when both protocols have matching seeds and run indices."),
        _render_links_html("Raw Statistical Exports", links),
    ]
    html = _html_page(
        "Paper Evaluation Statistics",
        "EGESS root: {} | Check-In root: {}".format(egess_root, checkin_root),
        _render_cards_html(cards),
        "".join(sections),
    )
    _write_text(out_dir / "index.html", html)


def main():
    parser = argparse.ArgumentParser(description="Generate statistical analysis for EGESS vs Check-In paper eval data.")
    parser.add_argument("--egess-root", default=str(ROOT_DIR / "paper_reports"), help="EGESS paper_reports root or a single suite folder")
    parser.add_argument("--checkin-root", default=str(ROOT_DIR / "external" / "checkin-egess-eval" / "paper_reports"), help="Check-In paper_reports root or a single suite folder")
    parser.add_argument("--out", default="", help="Output directory; defaults to statistics_reports/<timestamp>")
    args = parser.parse_args()

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out) if args.out else STAT_REPORTS_DIR / "paper_stats_{}".format(timestamp)
    out_dir.mkdir(parents=True, exist_ok=True)

    protocol_rows = _load_protocol_rows(args.egess_root, args.checkin_root)
    stats_rows = _stats_rows(protocol_rows)
    paired = _paired_rows(protocol_rows)
    overhead = _overhead_percentile_rows(stats_rows)
    boxplot = _boxplot_rows(protocol_rows)
    cdf = _cdf_rows(protocol_rows)
    histogram = _histogram_rows(protocol_rows)
    figures = _write_mean_ci_figures(out_dir, stats_rows)
    _write_raw_exports(out_dir, stats_rows, paired, overhead, boxplot, cdf, histogram)
    _write_text(
        out_dir / "README.md",
        "\n".join(
            [
                "# Paper Evaluation Statistics",
                "",
                "- Dashboard: `index.html`",
                "- Confidence intervals and percentiles: `metric_statistics.tsv`",
                "- Overhead percentiles: `overhead_percentiles.tsv`",
                "- Paired t-tests: `paired_t_tests.tsv`",
                "- Boxplot data: `boxplot_data.tsv`",
                "- CDF data: `cdf_points.tsv`",
                "- Histogram data: `histogram_bins.tsv`",
                "- Figure exports: `figure_exports/`",
            ]
        )
        + "\n",
    )
    _render_html(out_dir, stats_rows, paired, overhead, figures, args.egess_root, args.checkin_root)
    print(str(out_dir))


if __name__ == "__main__":
    main()
