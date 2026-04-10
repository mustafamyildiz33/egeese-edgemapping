#!/usr/bin/env python3
"""Phase-based paper evaluation runner for EGESS.

This runner reads a JSON phase specification, executes each requested demo with
an exact active scenario window, and writes Excel-friendly TSV plus Markdown
reports for the whole suite and for each individual run.
"""

import argparse
import csv
import json
import math
import os
import random
import statistics
import subprocess
import sys
import time
from html import escape
from pathlib import Path
from urllib import request


ROOT_DIR = Path(__file__).resolve().parent
RUNS_DIR = ROOT_DIR / "runs"
REPORTS_DIR = ROOT_DIR / "paper_reports"


SUMMARY_FIELDS = [
    "suite_id",
    "phase_id",
    "phase_name",
    "protocol",
    "challenge",
    "duration_sec",
    "active_duration_sec",
    "nodes",
    "run_index",
    "seed",
    "run_dir",
    "local_watch_port",
    "far_watch_port",
    "reachable_nodes",
    "total_nodes",
    "events_total",
    "fault_ops",
    "trigger_ops",
    "pull_rx_total",
    "push_rx_total",
    "pull_tx_total",
    "push_tx_total",
    "rx_bytes_total",
    "tx_bytes_total",
    "total_bytes",
    "total_mb",
    "tx_ok_total",
    "tx_fail_total",
    "tx_timeout_total",
    "tx_conn_error_total",
    "status",
]


WATCH_FIELDS = [
    "suite_id",
    "phase_id",
    "phase_name",
    "protocol",
    "challenge",
    "duration_sec",
    "nodes",
    "run_index",
    "seed",
    "view",
    "watch_port",
    "reachable",
    "protocol_state",
    "boundary_kind",
    "score",
    "front_score",
    "impact_score",
    "arrest_score",
    "coherence_score",
    "accepted_messages",
    "pull_rx",
    "push_rx",
    "pull_tx",
    "push_tx",
    "rx_total_bytes",
    "tx_total_bytes",
    "total_bytes",
    "total_mb",
    "direction_label",
    "phase",
    "distance_hops",
    "eta_cycles",
    "current_missing_count",
    "crash_sim",
    "lie_sensor",
    "flap",
]


SUMMARY_BY_NODES_FIELDS = ["phase_id", "challenge", "duration_sec", "nodes", "runs", "avg_total_mb", "avg_push_rx_total", "avg_tx_fail_total"]


RUN_OVERVIEW_FIELDS = [
    "phase_id",
    "challenge",
    "nodes",
    "run_index",
    "seed",
    "active_duration_sec",
    "reachable_nodes",
    "total_nodes",
    "events_total",
    "total_mb",
    "tx_fail_total",
    "tx_timeout_total",
    "status",
]


WATCH_OVERVIEW_FIELDS = [
    "view",
    "watch_port",
    "reachable",
    "protocol_state",
    "accepted_messages",
    "pull_rx",
    "push_rx",
    "pull_tx",
    "push_tx",
    "total_mb",
    "phase",
    "current_missing_count",
    "crash_sim",
    "lie_sensor",
    "flap",
]


NODE_FIELDS = [
    "port",
    "reachable",
    "protocol_state",
    "boundary_kind",
    "accepted_messages",
    "pull_rx",
    "push_rx",
    "pull_tx",
    "push_tx",
    "rx_total_bytes",
    "tx_total_bytes",
    "total_bytes",
    "total_mb",
    "direction_label",
    "phase",
    "current_missing_count",
    "crash_sim",
    "lie_sensor",
    "flap",
    "error",
]


HISTORY_FIELDS = [
    "sample_index",
    "sample_sec",
    "sample_label",
    "port",
    "reachable",
    "protocol_state",
    "accepted_messages",
    "pull_rx",
    "push_rx",
    "pull_tx",
    "push_tx",
    "rx_total_bytes",
    "tx_total_bytes",
    "total_bytes",
    "total_mb",
    "phase",
    "current_missing_count",
    "crash_sim",
    "lie_sensor",
    "flap",
    "error",
]


HISTORY_TOTAL_FIELDS = [
    "sample_index",
    "sample_sec",
    "sample_label",
    "reachable_nodes",
    "accepted_messages_total",
    "pull_rx_total",
    "push_rx_total",
    "pull_tx_total",
    "push_tx_total",
    "rx_bytes_total",
    "tx_bytes_total",
    "total_bytes",
    "total_mb",
]


SUMMARY_CHART_FIELDS = [
    "active_duration_sec",
    "reachable_nodes",
    "events_total",
    "fault_ops",
    "trigger_ops",
    "pull_rx_total",
    "push_rx_total",
    "pull_tx_total",
    "push_tx_total",
    "rx_bytes_total",
    "tx_bytes_total",
    "total_bytes",
    "total_mb",
    "tx_ok_total",
    "tx_fail_total",
    "tx_timeout_total",
    "tx_conn_error_total",
]


WATCH_CHART_FIELDS = [
    "accepted_messages",
    "pull_rx",
    "push_rx",
    "pull_tx",
    "push_tx",
    "total_bytes",
    "total_mb",
]


COMPARISON_FIELDS = [
    "scenario_label",
    "egess_setup",
    "egess_bytes",
    "egess_failures",
    "egess_detection_speed",
    "checkin_setup",
    "checkin_bytes",
    "checkin_failures",
    "checkin_detection_speed",
    "comparison_status",
    "comparison_note",
]


FIELD_LABELS = {
    "active_duration_sec": "Active Time",
    "accepted_messages": "Accepted Msgs",
    "checkin_bytes": "Check-In Bytes",
    "checkin_detection_speed": "Check-In Detection Speed",
    "checkin_failures": "Check-In Failures",
    "checkin_setup": "Check-In Setup",
    "avg_push_rx_total": "Avg Push RX",
    "avg_total_mb": "Avg MB",
    "avg_tx_fail_total": "Avg TX Fail",
    "avg": "Average",
    "challenge": "Challenge",
    "comparison_note": "Note",
    "comparison_status": "Compare",
    "coherence_score": "Coherence",
    "crash_sim": "Crash Sim",
    "current_missing_count": "Missing",
    "direction_label": "Direction",
    "distance_hops": "Distance",
    "duration_sec": "Duration",
    "egess_bytes": "EGESS Bytes",
    "egess_detection_speed": "EGESS Detection Speed",
    "egess_failures": "EGESS Failures",
    "egess_setup": "EGESS Setup",
    "eta_cycles": "ETA",
    "events_total": "Events",
    "far_watch_port": "Far Port",
    "fault_ops": "Fault Ops",
    "flap": "Flap",
    "front_score": "Front",
    "impact_score": "Impact",
    "lie_sensor": "Lie Sensor",
    "latest": "Latest",
    "local_watch_port": "Local Port",
    "max": "Max",
    "metric": "Metric",
    "min": "Min",
    "nodes": "Nodes",
    "phase_id": "Phase",
    "phase_name": "Phase Name",
    "port": "Port",
    "protocol_state": "State",
    "pull_rx": "Pull RX",
    "pull_rx_total": "Pull RX",
    "pull_tx": "Pull TX",
    "pull_tx_total": "Pull TX",
    "push_rx": "Push RX",
    "push_rx_total": "Push RX",
    "push_tx": "Push TX",
    "push_tx_total": "Push TX",
    "reachable_nodes": "Reachable",
    "reachable": "Reachable",
    "run_index": "Run",
    "rx_bytes_total": "RX Bytes",
    "score": "Score",
    "seed": "Seed",
    "sample_index": "Sample",
    "sample_label": "Sample Label",
    "sample_sec": "Sample Time",
    "scenario_label": "Scenario",
    "samples": "Samples",
    "suite_id": "Suite",
    "total_bytes": "Bytes",
    "total_mb": "MB",
    "total_nodes": "Total Nodes",
    "trigger_ops": "Triggers",
    "tx_conn_error_total": "Conn Err",
    "tx_fail_total": "TX Fail",
    "tx_ok_total": "TX OK",
    "tx_timeout_total": "TX Timeout",
    "tx_total_bytes": "TX Bytes",
    "view": "View",
    "watch_port": "Watch Port",
    "accepted_messages_total": "Accepted Msgs",
    "reachable_nodes": "Reachable",
    "error": "Error",
}


def _to_int(value, fallback):
    try:
        return int(value)
    except Exception:
        return int(fallback)


def _to_float(value, fallback):
    try:
        return float(value)
    except Exception:
        return float(fallback)


def _json_size_bytes(payload):
    """Return a compact UTF-8 JSON size for reporting."""
    try:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    except Exception:
        body = json.dumps(str(payload))
    return int(len(body.encode("utf-8")))


def _auto_grid_size(number_of_nodes):
    root = int(math.isqrt(int(number_of_nodes)))
    if root > 0 and root * root == int(number_of_nodes):
        return int(root)
    root = int(math.ceil(math.sqrt(float(number_of_nodes))))
    if root < 2:
        root = 2
    return int(root)


def _port_to_rc(base_port, port, grid):
    idx = int(port) - int(base_port)
    return int(idx // grid), int(idx % grid)


def _rc_to_port(base_port, row, col, grid, number_of_nodes):
    if row < 0 or col < 0 or row >= grid or col >= grid:
        return None
    idx = row * grid + col
    if idx < 0 or idx >= int(number_of_nodes):
        return None
    return int(base_port) + idx


def _hex_neighbors_odd_r(col, row, grid):
    if row % 2 == 0:
        candidates = [
            (col - 1, row),
            (col + 1, row),
            (col, row - 1),
            (col - 1, row - 1),
            (col, row + 1),
            (col - 1, row + 1),
        ]
    else:
        candidates = [
            (col - 1, row),
            (col + 1, row),
            (col + 1, row - 1),
            (col, row - 1),
            (col + 1, row + 1),
            (col, row + 1),
        ]
    out = []
    for c, r in candidates:
        if 0 <= c < grid and 0 <= r < grid:
            out.append((c, r))
    return out


def _hex_center_xy(row, col):
    x = math.sqrt(3.0) * (float(col) + (0.5 if int(row) % 2 == 1 else 0.0))
    y = 1.5 * float(row)
    return x, y


def _farthest_port(base_port, number_of_nodes, reference_port):
    grid = _auto_grid_size(number_of_nodes)
    ref_row, ref_col = _port_to_rc(base_port, reference_port, grid)
    ref_x, ref_y = _hex_center_xy(ref_row, ref_col)
    best_port = int(reference_port)
    best_distance = -1.0
    for port in range(int(base_port), int(base_port) + int(number_of_nodes)):
        row, col = _port_to_rc(base_port, port, grid)
        x, y = _hex_center_xy(row, col)
        d2 = ((x - ref_x) ** 2) + ((y - ref_y) ** 2)
        if d2 > best_distance:
            best_distance = d2
            best_port = int(port)
    return int(best_port)


def _center_port(base_port, number_of_nodes):
    grid = _auto_grid_size(number_of_nodes)
    row = int(grid // 2)
    col = int(grid // 2)
    center = _rc_to_port(base_port, row, col, grid, number_of_nodes)
    if center is None:
        center = int(base_port) + max(0, (int(number_of_nodes) // 2))
    return int(center)


def _neighbors_for_port(base_port, number_of_nodes, port):
    grid = _auto_grid_size(number_of_nodes)
    row, col = _port_to_rc(base_port, port, grid)
    neighbors = []
    for ncol, nrow in _hex_neighbors_odd_r(col, row, grid):
        nport = _rc_to_port(base_port, nrow, ncol, grid, number_of_nodes)
        if nport is not None and int(nport) != int(port):
            neighbors.append(int(nport))
    return sorted(neighbors)


def _post_json(port, payload, timeout=1.0):
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        "http://127.0.0.1:{}/".format(int(port)),
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _pull_state(port, origin="paper_eval", timeout=1.0):
    payload = {
        "op": "pull",
        "data": {"kind": "paper_eval"},
        "metadata": {"origin": str(origin)},
    }
    return _post_json(port, payload, timeout=timeout)


def _trigger_push(port, label, timeout=1.2):
    payload = {
        "op": "push",
        "data": {
            "type": "paper_eval_trigger",
            "label": str(label),
            "ts": float(time.time()),
        },
        "metadata": {
            "origin": int(port),
            "relay": 0,
            "forward_count": 0,
        },
    }
    return _post_json(port, payload, timeout=timeout)


def _inject_fault(port, fault, enable=True, period_sec=4, timeout=1.2):
    payload = {
        "op": "inject_fault",
        "data": {
            "fault": str(fault),
            "enable": bool(enable),
            "period_sec": int(period_sec),
        },
        "metadata": {"origin": "paper_eval"},
    }
    return _post_json(port, payload, timeout=timeout)


def _inject_state(port, sensor_state, timeout=1.2):
    payload = {
        "op": "inject_state",
        "data": {"sensor_state": str(sensor_state).strip().upper()},
        "metadata": {"origin": "paper_eval"},
    }
    return _post_json(port, payload, timeout=timeout)


def _append_jsonl(path, row):
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")


def _log_event(path, kind, data):
    row = {
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "kind": str(kind),
        "data": data,
    }
    _append_jsonl(path, row)


def _write_json(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _write_tsv(path, rows, fields):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\t".join(fields) + "\n")
        for row in rows:
            values = []
            for field in fields:
                value = row.get(field, "")
                if isinstance(value, float):
                    value = "{:.3f}".format(value)
                elif isinstance(value, (dict, list)):
                    value = json.dumps(value, sort_keys=True)
                values.append(str(value))
            handle.write("\t".join(values) + "\n")


def _field_label(field):
    label = FIELD_LABELS.get(str(field), "")
    if label:
        return label
    return str(field).replace("_", " ").title()


def _maybe_float(value):
    if isinstance(value, bool) or value in ("", None):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _maybe_int(value):
    number = _maybe_float(value)
    if number is None:
        return None
    if float(number).is_integer():
        return int(number)
    return None


def _boolish(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return None
    text = str(value).strip().lower()
    if text in ("true", "yes"):
        return True
    if text in ("false", "no"):
        return False
    return None


def _format_display_value(field, value):
    if value in ("", None):
        return ""

    truthy = _boolish(value)
    if truthy is not None:
        return "Yes" if truthy else "No"

    number_int = _maybe_int(value)
    number_float = _maybe_float(value)

    if field in ("duration_sec", "active_duration_sec") and number_float is not None:
        return "{:.3f}s".format(float(number_float))
    if field.endswith("_mb") and number_float is not None:
        return "{:.3f} MB".format(float(number_float))
    if field in ("rx_bytes_total", "tx_bytes_total", "total_bytes") and number_int is not None:
        return "{:,}".format(int(number_int))
    if field in (
        "accepted_messages",
        "events_total",
        "fault_ops",
        "trigger_ops",
        "pull_rx",
        "push_rx",
        "pull_tx",
        "push_tx",
        "pull_rx_total",
        "push_rx_total",
        "pull_tx_total",
        "push_tx_total",
        "reachable_nodes",
        "total_nodes",
        "run_index",
        "seed",
        "nodes",
        "watch_port",
        "local_watch_port",
        "far_watch_port",
        "tx_ok_total",
        "tx_fail_total",
        "tx_timeout_total",
        "tx_conn_error_total",
        "current_missing_count",
        "runs",
    ) and number_int is not None:
        return "{:,}".format(int(number_int))
    if field in (
        "score",
        "front_score",
        "impact_score",
        "arrest_score",
        "coherence_score",
        "avg_total_mb",
        "avg_push_rx_total",
        "avg_tx_fail_total",
        "distance_hops",
        "eta_cycles",
    ) and number_float is not None:
        return "{:.3f}".format(float(number_float))

    return str(value)


def _value_is_positive(field, value):
    number = _maybe_float(value)
    if number is None:
        return False
    if field in ("reachable_nodes", "total_nodes", "pull_rx", "push_rx", "pull_tx", "push_tx", "events_total", "accepted_messages"):
        return number > 0
    return number > 0


def _cell_class(field, value):
    classes = ["col-{}".format(str(field).replace("_", "-"))]
    number = _maybe_float(value)
    if field in ("tx_fail_total", "tx_timeout_total", "tx_conn_error_total", "current_missing_count", "avg_tx_fail_total"):
        classes.append("metric-bad" if number and number > 0 else "metric-good")
    elif field in ("egess_failures", "checkin_failures"):
        classes.append("metric-bad")
    elif field in ("total_mb", "avg_total_mb", "rx_bytes_total", "tx_bytes_total", "total_bytes"):
        classes.append("metric-accent")
    elif field in ("egess_bytes", "checkin_bytes", "egess_detection_speed", "checkin_detection_speed"):
        classes.append("metric-accent")
    elif field in ("events_total", "accepted_messages", "pull_rx", "push_rx", "pull_tx", "push_tx", "pull_rx_total", "push_rx_total", "pull_tx_total", "push_tx_total"):
        if _value_is_positive(field, value):
            classes.append("metric-ink")
    return " ".join(classes)


def _badge_class(field, value):
    text = str(value).strip()
    lower_text = text.lower()
    truthy = _boolish(value)

    if field == "status":
        if lower_text == "ok":
            return "pill-good"
        if lower_text in ("warn", "warning"):
            return "pill-warn"
        return "pill-bad"
    if field == "comparison_status":
        if lower_text == "fair":
            return "pill-good"
        if lower_text == "mismatch":
            return "pill-warn"
        return "pill-bad"
    if field == "view":
        if text.upper() == "LOCAL":
            return "pill-local"
        if text.upper() == "FAR":
            return "pill-far"
    if field in ("protocol_state", "phase"):
        return "pill-soft"
    if field == "reachable" and truthy is not None:
        return "pill-good" if truthy else "pill-bad"
    if field in ("crash_sim", "lie_sensor", "flap") and truthy is not None:
        return "pill-bad" if truthy else "pill-off"
    return ""


def _row_class(row):
    classes = []
    if str(row.get("status", "")).strip().lower() == "ok":
        classes.append("row-ok")
    elif str(row.get("status", "")).strip():
        classes.append("row-alert")
    if str(row.get("comparison_status", "")).strip().lower() == "fair":
        classes.append("row-ok")
    elif str(row.get("comparison_status", "")).strip():
        classes.append("row-alert")
    if str(row.get("view", "")).strip().upper() == "LOCAL":
        classes.append("row-local")
    elif str(row.get("view", "")).strip().upper() == "FAR":
        classes.append("row-far")
    return " ".join(classes)


def _render_table_html(title, rows, fields, subtitle=""):
    head = []
    for field in fields:
        head.append("<th>{}</th>".format(escape(_field_label(field))))

    body = []
    if rows:
        for row in rows:
            cells = []
            for field in fields:
                display = escape(_format_display_value(field, row.get(field, "")))
                badge_class = _badge_class(field, row.get(field, ""))
                if badge_class:
                    display = '<span class="badge {}">{}</span>'.format(badge_class, display)
                cells.append('<td class="{}">{}</td>'.format(_cell_class(field, row.get(field, "")), display))
            body.append('<tr class="{}">{}</tr>'.format(_row_class(row), "".join(cells)))
    else:
        body.append('<tr><td colspan="{}" class="empty">No rows recorded.</td></tr>'.format(len(fields)))

    subtitle_html = ""
    if subtitle:
        subtitle_html = '<p class="section-note">{}</p>'.format(escape(subtitle))

    return """<section class="panel">
<div class="panel-head">
  <h2>{title}</h2>
  {subtitle}
</div>
<div class="table-wrap">
  <table>
    <thead><tr>{head}</tr></thead>
    <tbody>{body}</tbody>
  </table>
</div>
</section>""".format(
        title=escape(title),
        subtitle=subtitle_html,
        head="".join(head),
        body="".join(body),
    )


def _render_cards_html(cards):
    chunks = []
    for card in cards:
        tone = str(card.get("tone", "neutral")).strip()
        chunks.append(
            """<div class="stat-card {tone}">
  <div class="stat-label">{label}</div>
  <div class="stat-value">{value}</div>
  <div class="stat-note">{note}</div>
</div>""".format(
                tone=escape(tone),
                label=escape(str(card.get("label", ""))),
                value=escape(str(card.get("value", ""))),
                note=escape(str(card.get("note", ""))),
            )
        )
    return '<section class="card-grid">{}</section>'.format("".join(chunks))


def _render_links_html(title, links):
    items = []
    for href, label in links:
        items.append('<li><a href="{href}">{label}</a></li>'.format(href=escape(str(href)), label=escape(str(label))))
    return """<section class="panel">
<div class="panel-head">
  <h2>{}</h2>
</div>
<ul class="link-list">{}</ul>
</section>""".format(escape(title), "".join(items))


def _render_run_deep_dive_html(report_dir, summary_rows):
    if not summary_rows:
        return """<section class="panel">
<div class="panel-head">
  <h2>Run Deep Dives</h2>
  <p class="section-note">Node Spotlight lives on the per-run dashboard, but no runs are available yet.</p>
</div>
</section>"""

    cards = []
    for row in sorted(summary_rows, key=lambda item: (int(item.get("nodes", 0)), int(item.get("run_index", 0)))):
        run_path = ROOT_DIR / str(row.get("run_dir", "")).strip() / "paper_summary.html"
        href = os.path.relpath(run_path, start=report_dir)
        status = _format_display_value("status", row.get("status", ""))
        badge_class = _badge_class("status", row.get("status", "")) or "pill-soft"
        cards.append(
            """<article class="run-link-card">
  <div class="run-link-top">
    <div>
      <div class="run-link-title">Run {run_index} · N{nodes}</div>
      <div class="run-link-meta">Seed {seed} · {challenge}</div>
    </div>
    <span class="badge {badge_class}">{status}</span>
  </div>
  <div class="run-link-stats">
    <span>{active_time}</span>
    <span>{traffic}</span>
    <span>{failures} TX fail</span>
  </div>
  <a class="run-link-action" href="{href}">Open Node Spotlight</a>
</article>""".format(
                run_index=escape(_format_display_value("run_index", row.get("run_index", ""))),
                nodes=escape(_format_display_value("nodes", row.get("nodes", ""))),
                seed=escape(_format_display_value("seed", row.get("seed", ""))),
                challenge=escape(_format_display_value("challenge", row.get("challenge", ""))),
                badge_class=escape(badge_class),
                status=escape(status),
                active_time=escape(_format_display_value("active_duration_sec", row.get("active_duration_sec", ""))),
                traffic=escape(_format_display_value("total_mb", row.get("total_mb", ""))),
                failures=escape(_format_display_value("tx_fail_total", row.get("tx_fail_total", ""))),
                href=escape(href),
            )
        )

    return """<section class="panel">
<div class="panel-head">
  <h2>Run Deep Dives</h2>
  <p class="section-note">Open any run below to use Node Spotlight, inspect a specific port, and see that node's counters and history.</p>
</div>
<div class="run-link-grid">{}</div>
</section>""".format("".join(cards))


def _html_page(title, subtitle, cards_html, sections_html, script_html=""):
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      --bg: #f5f7fb;
      --panel: rgba(255, 255, 255, 0.92);
      --panel-strong: #ffffff;
      --ink: #18212f;
      --muted: #5d6b82;
      --line: #d9e1ef;
      --blue: #2474e5;
      --blue-soft: #e8f1ff;
      --teal: #118a7e;
      --teal-soft: #e6fbf5;
      --gold: #c58f10;
      --gold-soft: #fff5d8;
      --red: #c73a3a;
      --red-soft: #ffe6e6;
      --purple-soft: #f1ebff;
      --orange-soft: #fff0e4;
      --shadow: 0 18px 48px rgba(22, 37, 66, 0.10);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(36, 116, 229, 0.12), transparent 28%),
        radial-gradient(circle at top right, rgba(17, 138, 126, 0.12), transparent 30%),
        linear-gradient(180deg, #fbfcff 0%, var(--bg) 100%);
    }}
    .page {{
      max-width: 1380px;
      margin: 0 auto;
      padding: 28px 24px 48px;
    }}
    .hero {{
      background: linear-gradient(135deg, rgba(36, 116, 229, 0.98), rgba(17, 138, 126, 0.92));
      color: #ffffff;
      border-radius: 24px;
      padding: 24px 28px;
      box-shadow: var(--shadow);
      margin-bottom: 22px;
    }}
    .hero h1 {{
      margin: 0 0 6px;
      font-size: 2rem;
      line-height: 1.1;
    }}
    .hero p {{
      margin: 0;
      color: rgba(255, 255, 255, 0.90);
      font-size: 1rem;
    }}
    .card-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin-bottom: 20px;
    }}
    .stat-card {{
      background: var(--panel-strong);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px 18px;
      box-shadow: var(--shadow);
    }}
    .stat-card.good {{ background: linear-gradient(180deg, var(--panel-strong), #f2fffb); }}
    .stat-card.warn {{ background: linear-gradient(180deg, var(--panel-strong), #fffaf0); }}
    .stat-card.bad {{ background: linear-gradient(180deg, var(--panel-strong), #fff4f4); }}
    .stat-card.accent {{ background: linear-gradient(180deg, var(--panel-strong), #f3f7ff); }}
    .stat-label {{
      color: var(--muted);
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 6px;
    }}
    .stat-value {{
      font-size: 1.55rem;
      font-weight: 700;
      margin-bottom: 4px;
    }}
    .stat-note {{
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid rgba(217, 225, 239, 0.9);
      border-radius: 22px;
      padding: 18px;
      box-shadow: var(--shadow);
      margin-bottom: 18px;
      backdrop-filter: blur(6px);
    }}
    .panel-head {{
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
      flex-wrap: wrap;
    }}
    .panel h2 {{
      margin: 0;
      font-size: 1.1rem;
    }}
    .section-note {{
      margin: 0;
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .table-wrap {{
      overflow-x: auto;
      border-radius: 16px;
      border: 1px solid var(--line);
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      min-width: 840px;
      background: #ffffff;
    }}
    th, td {{
      padding: 11px 12px;
      border-bottom: 1px solid #e8edf6;
      text-align: left;
      white-space: nowrap;
      vertical-align: top;
    }}
    th {{
      position: sticky;
      top: 0;
      z-index: 1;
      background: #eef4ff;
      color: #294067;
      font-size: 0.84rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    tbody tr:nth-child(even) {{
      background: rgba(245, 248, 253, 0.75);
    }}
    .row-local {{
      background: linear-gradient(90deg, rgba(255, 240, 228, 0.85), rgba(255, 255, 255, 0));
    }}
    .row-far {{
      background: linear-gradient(90deg, rgba(232, 241, 255, 0.85), rgba(255, 255, 255, 0));
    }}
    .badge {{
      display: inline-block;
      border-radius: 999px;
      padding: 4px 10px;
      font-weight: 700;
      font-size: 0.84rem;
    }}
    .pill-good {{ background: var(--teal-soft); color: #10695f; }}
    .pill-warn {{ background: var(--gold-soft); color: #7a5905; }}
    .pill-bad {{ background: var(--red-soft); color: #922d2d; }}
    .pill-local {{ background: var(--orange-soft); color: #8b4c12; }}
    .pill-far {{ background: var(--blue-soft); color: #1a57b2; }}
    .pill-soft {{ background: var(--purple-soft); color: #5c3f9f; }}
    .pill-off {{ background: #eef2f8; color: #5d6b82; }}
    .metric-accent {{ color: #125bb8; font-weight: 700; }}
    .metric-bad {{ color: #b03232; font-weight: 700; }}
    .metric-good {{ color: #0f7a5b; font-weight: 700; }}
    .metric-ink {{ font-weight: 650; }}
    .chart-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 14px;
    }}
    .chart-card {{
      background: linear-gradient(180deg, #ffffff, #f9fbff);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
    }}
    .chart-title {{
      font-weight: 700;
      margin-bottom: 4px;
    }}
    .chart-stats,
    .chart-footer,
    .chart-empty {{
      color: var(--muted);
      font-size: 0.9rem;
    }}
    .chart-footer {{
      margin-top: 8px;
    }}
    .metric-chart {{
      width: 100%;
      height: auto;
      margin-top: 8px;
      display: block;
    }}
    .chart-axis {{
      stroke: #cfd8e8;
      stroke-width: 1;
    }}
    .guide-grid,
    .control-grid,
    .spotlight-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
    }}
    .guide-card,
    .control-card,
    .spotlight-card {{
      background: linear-gradient(180deg, #ffffff, #f8fbff);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
    }}
    .guide-card h3,
    .control-card h3,
    .spotlight-card h3 {{
      margin: 0 0 8px;
      font-size: 1rem;
    }}
    .guide-card p,
    .control-card p,
    .spotlight-card p {{
      margin: 0;
      color: var(--muted);
      line-height: 1.45;
    }}
    .guide-table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 12px;
    }}
    .guide-table th,
    .guide-table td {{
      padding: 10px 12px;
      border-bottom: 1px solid #e8edf6;
      text-align: left;
      vertical-align: top;
    }}
    .guide-table th {{
      background: #eef4ff;
      color: #294067;
      font-size: 0.84rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    .control-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 8px;
      align-items: end;
    }}
    .control-field {{
      display: flex;
      flex-direction: column;
      gap: 6px;
      min-width: 170px;
      flex: 1 1 190px;
    }}
    .control-field label {{
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--muted);
      font-weight: 700;
    }}
    .control-field input,
    .control-field select {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      font: inherit;
      background: #ffffff;
      color: var(--ink);
      box-shadow: inset 0 1px 2px rgba(24, 33, 47, 0.04);
    }}
    .micro-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 10px;
      margin-top: 12px;
    }}
    .micro-card {{
      background: linear-gradient(180deg, #f8fbff, #ffffff);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px;
    }}
    .micro-label {{
      color: var(--muted);
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 4px;
    }}
    .micro-value {{
      font-weight: 700;
      font-size: 1.12rem;
    }}
    .micro-note {{
      color: var(--muted);
      font-size: 0.88rem;
      margin-top: 4px;
    }}
    .spotlight-log {{
      list-style: none;
      padding: 0;
      margin: 12px 0 0;
      display: grid;
      gap: 8px;
    }}
    .spotlight-log li {{
      background: rgba(239, 244, 255, 0.9);
      border: 1px solid #dbe5f6;
      border-radius: 12px;
      padding: 8px 10px;
      font-size: 0.92rem;
      line-height: 1.4;
    }}
    .accent-note {{
      color: #1f63c6;
      font-weight: 700;
    }}
    .empty {{
      text-align: center;
      color: var(--muted);
      padding: 18px;
    }}
    .link-list {{
      margin: 0;
      padding-left: 18px;
    }}
    .link-list li {{
      margin: 8px 0;
    }}
    .link-list a {{
      color: var(--blue);
      text-decoration: none;
      font-weight: 600;
    }}
    .link-list a:hover {{
      text-decoration: underline;
    }}
    .run-link-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 16px;
    }}
    .run-link-card {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
      background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(247,250,255,0.98));
      box-shadow: 0 10px 24px rgba(24, 33, 47, 0.06);
    }}
    .run-link-top {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
    }}
    .run-link-title {{
      font-size: 1.05rem;
      font-weight: 700;
      margin-bottom: 4px;
    }}
    .run-link-meta {{
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .run-link-stats {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 14px;
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .run-link-stats span {{
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(36, 116, 229, 0.08);
    }}
    .run-link-action {{
      display: inline-block;
      padding: 10px 14px;
      border-radius: 12px;
      background: linear-gradient(135deg, #2474e5, #118a7e);
      color: white;
      text-decoration: none;
      font-weight: 700;
    }}
    .run-link-action:hover {{
      opacity: 0.92;
    }}
    .scenario-tab-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 14px;
    }}
    .scenario-tab {{
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #ffffff;
      color: var(--muted);
      padding: 8px 14px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      box-shadow: 0 4px 14px rgba(24, 33, 47, 0.05);
    }}
    .scenario-tab.active {{
      background: linear-gradient(135deg, #2474e5, #118a7e);
      color: #ffffff;
      border-color: transparent;
    }}
    details {{
      margin-top: 14px;
    }}
    summary {{
      cursor: pointer;
      color: var(--blue);
      font-weight: 700;
    }}
    @media (max-width: 720px) {{
      .page {{ padding: 16px 14px 36px; }}
      .hero {{ padding: 18px 18px; border-radius: 18px; }}
      .hero h1 {{ font-size: 1.55rem; }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <h1>{title}</h1>
      <p>{subtitle}</p>
    </section>
    {cards}
    {sections}
  </main>
  {script_html}
</body>
</html>""".format(
        title=escape(str(title)),
        subtitle=escape(str(subtitle)),
        cards=cards_html,
        sections=sections_html,
        script_html=script_html,
    )


def _write_text(path, content):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)


def _load_jsonl(path):
    rows = []
    file_path = Path(path)
    if not file_path.exists():
        return rows
    with open(file_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _node_row_from_state(port, reachable, state=None, counters=None, error=""):
    state = state if isinstance(state, dict) else {}
    counters = counters if isinstance(counters, dict) else {}
    layer2 = state.get("layer2_confirmation", {}) if isinstance(state.get("layer2_confirmation"), dict) else {}
    faults = state.get("faults", {}) if isinstance(state.get("faults"), dict) else {}
    total_bytes = _to_int(counters.get("rx_total_bytes", 0), 0) + _to_int(counters.get("tx_total_bytes", 0), 0)
    return {
        "port": int(port),
        "reachable": bool(reachable),
        "protocol_state": str(state.get("protocol_state", "")),
        "boundary_kind": str(state.get("boundary_kind", "")),
        "accepted_messages": _to_int(state.get("accepted_messages", 0), 0),
        "pull_rx": _to_int(counters.get("pull_rx", 0), 0),
        "push_rx": _to_int(counters.get("push_rx", 0), 0),
        "pull_tx": _to_int(counters.get("pull_tx", 0), 0),
        "push_tx": _to_int(counters.get("push_tx", 0), 0),
        "rx_total_bytes": _to_int(counters.get("rx_total_bytes", 0), 0),
        "tx_total_bytes": _to_int(counters.get("tx_total_bytes", 0), 0),
        "total_bytes": int(total_bytes),
        "total_mb": round(float(total_bytes) / 1048576.0, 3),
        "direction_label": str(layer2.get("direction_label", "")),
        "phase": str(layer2.get("phase", "")),
        "current_missing_count": len(state.get("current_missing_neighbors", [])) if isinstance(state.get("current_missing_neighbors"), list) else 0,
        "crash_sim": bool(faults.get("crash_sim", False)),
        "lie_sensor": bool(faults.get("lie_sensor", False)),
        "flap": bool(faults.get("flap", False)),
        "error": str(error or ""),
    }


def _all_node_rows(evidence):
    out = []
    for port in sorted(evidence.get("nodes", {}).keys(), key=lambda item: int(item)):
        node_info = evidence.get("nodes", {}).get(str(port), {})
        out.append(
            _node_row_from_state(
                port=port,
                reachable=node_info.get("reachable", False),
                state=node_info.get("state", {}),
                counters=node_info.get("msg_counters", {}),
                error=node_info.get("error", ""),
            )
        )
    return out


def _sample_nodes(base_port, number_of_nodes, sample_index, sample_sec):
    rows = []
    totals = {
        "sample_index": int(sample_index),
        "sample_sec": round(float(sample_sec), 3),
        "sample_label": "t+{:.1f}s".format(float(sample_sec)),
        "reachable_nodes": 0,
        "accepted_messages_total": 0,
        "pull_rx_total": 0,
        "push_rx_total": 0,
        "pull_tx_total": 0,
        "push_tx_total": 0,
        "rx_bytes_total": 0,
        "tx_bytes_total": 0,
        "total_bytes": 0,
        "total_mb": 0.0,
    }

    for port in range(int(base_port), int(base_port) + int(number_of_nodes)):
        try:
            res = _pull_state(port, origin="paper_history", timeout=0.75)
            state = res.get("data", {}).get("node_state", {}) if isinstance(res, dict) else {}
            counters = state.get("msg_counters", {}) if isinstance(state.get("msg_counters"), dict) else {}
            row = _node_row_from_state(port, True, state=state, counters=counters)
            totals["reachable_nodes"] += 1
        except Exception as exc:
            row = _node_row_from_state(port, False, state={}, counters={}, error=str(exc))

        row.update(
            {
                "sample_index": int(sample_index),
                "sample_sec": round(float(sample_sec), 3),
                "sample_label": "t+{:.1f}s".format(float(sample_sec)),
            }
        )
        rows.append(row)
        totals["accepted_messages_total"] += _to_int(row.get("accepted_messages", 0), 0)
        totals["pull_rx_total"] += _to_int(row.get("pull_rx", 0), 0)
        totals["push_rx_total"] += _to_int(row.get("push_rx", 0), 0)
        totals["pull_tx_total"] += _to_int(row.get("pull_tx", 0), 0)
        totals["push_tx_total"] += _to_int(row.get("push_tx", 0), 0)
        totals["rx_bytes_total"] += _to_int(row.get("rx_total_bytes", 0), 0)
        totals["tx_bytes_total"] += _to_int(row.get("tx_total_bytes", 0), 0)
        totals["total_bytes"] += _to_int(row.get("total_bytes", 0), 0)

    totals["total_mb"] = round(float(totals["total_bytes"]) / 1048576.0, 3)
    return rows, totals


def _metric_summary_rows(rows, fields):
    out = []
    for field in fields:
        values = []
        for row in rows:
            number = _maybe_float(row.get(field))
            if number is not None:
                values.append(float(number))
        if not values:
            continue
        out.append(
            {
                "metric": _field_label(field),
                "field": str(field),
                "samples": len(values),
                "avg": round(statistics.mean(values), 3),
                "min": round(min(values), 3),
                "max": round(max(values), 3),
                "latest": round(values[-1], 3),
            }
        )
    return out


def _series_points(rows, field, label_fn):
    points = []
    for idx, row in enumerate(rows):
        number = _maybe_float(row.get(field))
        if number is None:
            continue
        points.append((str(label_fn(row, idx)), float(number)))
    return points


def _series_svg(points, color):
    if not points:
        return '<div class="chart-empty">No data</div>'

    width = 360.0
    height = 150.0
    pad_x = 14.0
    pad_top = 14.0
    pad_bottom = 22.0
    pad_y = 10.0
    values = [float(value) for _, value in points]
    min_v = min(values)
    max_v = max(values)
    if abs(max_v - min_v) < 1e-9:
        max_v = min_v + 1.0
    usable_w = width - (pad_x * 2.0)
    usable_h = height - pad_top - pad_bottom - pad_y
    step_x = usable_w / max(1, len(points) - 1)

    coords = []
    fill_coords = []
    for idx, (_, value) in enumerate(points):
        x = pad_x + (idx * step_x if len(points) > 1 else usable_w / 2.0)
        ratio = (float(value) - min_v) / (max_v - min_v)
        y = pad_top + ((1.0 - ratio) * usable_h)
        coords.append((x, y))
        fill_coords.append("{:.2f},{:.2f}".format(x, y))

    polyline = " ".join("{:.2f},{:.2f}".format(x, y) for x, y in coords)
    fill_poly = " ".join(
        ["{:.2f},{:.2f}".format(coords[0][0], height - pad_bottom)]
        + fill_coords
        + ["{:.2f},{:.2f}".format(coords[-1][0], height - pad_bottom)]
    )
    circles = "".join(
        '<circle cx="{:.2f}" cy="{:.2f}" r="2.8" fill="{}"></circle>'.format(x, y, escape(color))
        for x, y in coords
    )
    return """<svg viewBox="0 0 {w} {h}" class="metric-chart" role="img" aria-label="metric chart">
  <line x1="{px}" y1="{base}" x2="{wx}" y2="{base}" class="chart-axis"></line>
  <line x1="{px}" y1="{pt}" x2="{px}" y2="{base}" class="chart-axis"></line>
  <polygon points="{fill_poly}" fill="{color}" opacity="0.12"></polygon>
  <polyline points="{polyline}" fill="none" stroke="{color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>
  {circles}
</svg>""".format(
        w=int(width),
        h=int(height),
        px=pad_x,
        pt=pad_top,
        wx=width - pad_x,
        base=height - pad_bottom,
        fill_poly=fill_poly,
        polyline=polyline,
        circles=circles,
        color=escape(color),
    )


def _render_chart_grid_html(title, rows, fields, label_fn, subtitle=""):
    colors = ["#2474e5", "#118a7e", "#c58f10", "#c73a3a", "#8b4cd6", "#ff7a59"]
    charts = []
    for idx, field in enumerate(fields):
        points = _series_points(rows, field, label_fn)
        if not points:
            continue
        values = [value for _, value in points]
        charts.append(
            """<div class="chart-card">
  <div class="chart-title">{title}</div>
  <div class="chart-stats">avg {avg} | min {minv} | max {maxv}</div>
  {svg}
  <div class="chart-footer">{first} to {last}</div>
</div>""".format(
                title=escape(_field_label(field)),
                avg=escape(_format_display_value(field, round(statistics.mean(values), 3))),
                minv=escape(_format_display_value(field, round(min(values), 3))),
                maxv=escape(_format_display_value(field, round(max(values), 3))),
                svg=_series_svg(points, colors[idx % len(colors)]),
                first=escape(points[0][0]),
                last=escape(points[-1][0]),
            )
        )

    subtitle_html = '<p class="section-note">{}</p>'.format(escape(subtitle)) if subtitle else ""
    content = "".join(charts) if charts else '<div class="chart-empty">No chart data available.</div>'
    return """<section class="panel">
<div class="panel-head">
  <h2>{title}</h2>
  {subtitle}
</div>
<div class="chart-grid">{content}</div>
</section>""".format(title=escape(title), subtitle=subtitle_html, content=content)


def _run_label(row, _idx):
    return "N{} R{}".format(_format_display_value("nodes", row.get("nodes", "")), _format_display_value("run_index", row.get("run_index", "")))


def _sample_label(row, _idx):
    return str(row.get("sample_label", ""))


def _node_spotlight_payload(evidence):
    payload = []
    for port in sorted(evidence.get("nodes", {}).keys(), key=lambda item: int(item)):
        node_info = evidence.get("nodes", {}).get(str(port), {})
        state = node_info.get("state", {}) if isinstance(node_info.get("state"), dict) else {}
        counters = node_info.get("msg_counters", {}) if isinstance(node_info.get("msg_counters"), dict) else {}
        row = _node_row_from_state(
            port=port,
            reachable=node_info.get("reachable", False),
            state=state,
            counters=counters,
            error=node_info.get("error", ""),
        )
        row["recent_msgs"] = state.get("recent_msgs", [])[-15:] if isinstance(state.get("recent_msgs"), list) else []
        row["recent_alerts"] = state.get("recent_alerts", [])[-10:] if isinstance(state.get("recent_alerts"), list) else []
        row["pull_cycles"] = _to_int(state.get("pull_cycles", 0), 0)
        row["incoming_events_count"] = len(state.get("incoming_events", [])) if isinstance(state.get("incoming_events"), list) else 0
        row["known_nodes_count"] = len(state.get("known_nodes", [])) if isinstance(state.get("known_nodes"), list) else 0
        payload.append(row)
    return payload


def _render_glossary_html():
    cards = [
        (
            "LOCAL vs FAR",
            "LOCAL is the watched node closest to the scenario focus. FAR is the watched node farthest from LOCAL, which helps show propagation instead of direct impact.",
        ),
        (
            "Speed",
            "Use active time, early chart slope, accepted messages, and pull or push growth to see how quickly information spreads and stabilizes.",
        ),
        (
            "Resilience",
            "Use reachable nodes, TX failures, TX timeouts, missing neighbors, and active fault flags to judge how well the protocol handles stress.",
        ),
        (
            "Overhead",
            "Use total bytes, total MB, pull RX or TX, and push RX or TX to measure message cost on a node or across a run.",
        ),
        (
            "Scalability",
            "Compare the grouped node-count table and suite charts as 49, 64, or 81 nodes grow. Linear growth is usually easier to defend in the paper.",
        ),
        (
            "Correlation",
            "Treat correlation as pattern matching, not proof. If TX failures rise with MB, retries are likely adding cost. If accepted messages rise with push RX, dissemination is intensifying.",
        ),
    ]

    guide_rows = [
        ("Speed", "active_duration_sec, accepted_messages, pull or push history", "Faster runs usually show earlier, steeper growth with fewer stalls."),
        ("Resilience", "reachable_nodes, tx_fail_total, tx_timeout_total, current_missing_count", "More failures or missing neighbors usually means weaker fault tolerance."),
        ("Overhead", "total_mb, total_bytes, pull_rx or tx, push_rx or tx", "Lower MB with similar coverage is a stronger efficiency story."),
        ("Accuracy", "phase, direction_label, boundary_kind, false-positive metrics when present", "Consistency and correct state interpretation support hazard sensing claims."),
        ("Correlation", "compare chart pairs like failures vs MB, accepted_messages vs push_rx", "Look for metrics rising or falling together, then confirm with scenario context."),
    ]

    card_html = "".join(
        '<div class="guide-card"><h3>{}</h3><p>{}</p></div>'.format(escape(title), escape(body))
        for title, body in cards
    )
    row_html = "".join(
        "<tr><td>{}</td><td>{}</td><td>{}</td></tr>".format(escape(goal), escape(metrics), escape(reading))
        for goal, metrics, reading in guide_rows
    )
    return """<section class="panel">
<div class="panel-head">
  <h2>Metric Guide</h2>
  <p class="section-note">This is the legend for reading the dashboard and the paper figures.</p>
</div>
<div class="guide-grid">{}</div>
<table class="guide-table">
  <thead><tr><th>Goal</th><th>Main Metrics</th><th>How To Read It</th></tr></thead>
  <tbody>{}</tbody>
</table>
</section>""".format(card_html, row_html)


def _read_tsv_rows(path):
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def _comparison_report_roots():
    candidates = [
        REPORTS_DIR,
        ROOT_DIR / "external" / "checkin-egess-eval" / "paper_reports",
        ROOT_DIR.parent.parent / "paper_reports",
    ]
    roots = []
    seen = set()
    for path in candidates:
        if not path.exists() or not path.is_dir():
            continue
        resolved = str(path.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        roots.append(path)
    return roots


def _scenario_label(phase_id, challenge):
    labels = {
        "steady_state_baseline": "Baseline",
        "firebomb": "Fire",
        "tornado_sweep": "Tornado",
        "ghost_outage_noise": "Ghost Outage + Noise",
    }
    text = labels.get(str(challenge).strip(), "")
    if text:
        return text
    if str(challenge).strip():
        return str(challenge).replace("_", " ").title()
    return str(phase_id).replace("_", " ").title()


def _scenario_sort_key(signature):
    phase_id, challenge = signature
    phase_order = {"phase1": 1, "phase2": 2, "phase3": 3}
    return (phase_order.get(str(phase_id), 99), _scenario_label(phase_id, challenge))


def _suite_setup_parts(rows):
    nodes = sorted({_to_int(row.get("nodes", 0), 0) for row in rows if _to_int(row.get("nodes", 0), 0) > 0})
    durations = sorted({int(round(_to_float(row.get("duration_sec", 0.0), 0.0))) for row in rows if _to_float(row.get("duration_sec", 0.0), 0.0) > 0})
    node_label = "mixed"
    if len(nodes) == 1:
        node_label = "N{}".format(nodes[0])
    elif nodes:
        node_label = "N{}".format("/".join(str(value) for value in nodes))
    duration_label = "mixed"
    if len(durations) == 1:
        duration_label = "{}s".format(durations[0])
    elif durations:
        duration_label = "/".join("{}s".format(value) for value in durations)
    return node_label, duration_label, tuple(nodes), tuple(durations)


def _row_has_signal(row):
    state = str(row.get("protocol_state", "")).strip().upper()
    phase = str(row.get("phase", "")).strip().upper()
    if state and state not in ("NORMAL", "STABLE"):
        return True
    if phase and phase not in ("", "CLEAR"):
        return True
    if _to_int(row.get("accepted_messages", 0), 0) > 0:
        return True
    if _to_int(row.get("pull_rx", 0), 0) > 0 or _to_int(row.get("push_rx", 0), 0) > 0:
        return True
    if _to_int(row.get("total_bytes", 0), 0) > 0:
        return True
    if _to_int(row.get("current_missing_count", 0), 0) > 0:
        return True
    for field in ("crash_sim", "lie_sensor", "flap"):
        truthy = _boolish(row.get(field, ""))
        if truthy:
            return True
    return False


def _run_detection_speed_sec(repo_root, summary_row):
    run_dir_value = str(summary_row.get("run_dir", "")).strip()
    if not run_dir_value:
        return None
    local_port = _to_int(summary_row.get("local_watch_port", 0), 0)
    if local_port <= 0:
        return None
    history_path = repo_root / run_dir_value / "paper_pull_history.tsv"
    history_rows = _read_tsv_rows(history_path)
    if not history_rows:
        return None
    port_rows = []
    for row in history_rows:
        if _to_int(row.get("port", -1), -1) != local_port:
            continue
        if str(row.get("error", "")).strip():
            continue
        port_rows.append(row)
    if not port_rows:
        return None
    port_rows.sort(key=lambda row: (_to_int(row.get("sample_index", 0), 0), _to_float(row.get("sample_sec", 0.0), 0.0)))
    if _row_has_signal(port_rows[0]):
        return 0.0
    for row in port_rows[1:]:
        if _row_has_signal(row):
            return _to_float(row.get("sample_sec", 0.0), 0.0)
    return None


def _latest_protocol_suite_index():
    suites_by_protocol = {}
    for report_root in _comparison_report_roots():
        repo_root = report_root.parent
        for suite_dir in sorted(report_root.iterdir()):
            if not suite_dir.is_dir():
                continue
            rows = _read_tsv_rows(suite_dir / "all_runs.tsv")
            if not rows:
                continue
            row0 = rows[0]
            protocol = str(row0.get("protocol", "")).strip().lower()
            if not protocol:
                continue
            signature = (str(row0.get("phase_id", "")).strip(), str(row0.get("challenge", "")).strip())
            entry = {
                "report_dir": suite_dir,
                "repo_root": repo_root,
                "rows": rows,
                "mtime": suite_dir.stat().st_mtime,
            }
            protocol_map = suites_by_protocol.setdefault(protocol, {})
            existing = protocol_map.get(signature)
            if existing is None or entry["mtime"] > existing["mtime"]:
                protocol_map[signature] = entry
    return suites_by_protocol


def _suite_comparison_metrics(entry):
    rows = entry.get("rows", [])
    repo_root = entry.get("repo_root")
    if not rows or repo_root is None:
        return None
    total_bytes_values = [_to_float(row.get("total_bytes", 0.0), 0.0) for row in rows]
    total_mb_values = [_to_float(row.get("total_mb", 0.0), 0.0) for row in rows]
    failure_values = [
        _to_float(row.get("tx_fail_total", 0.0), 0.0)
        + _to_float(row.get("tx_timeout_total", 0.0), 0.0)
        + _to_float(row.get("tx_conn_error_total", 0.0), 0.0)
        for row in rows
    ]
    detection_values = []
    for row in rows:
        detection = _run_detection_speed_sec(repo_root, row)
        if detection is not None:
            detection_values.append(float(detection))
    node_label, duration_label, node_key, duration_key = _suite_setup_parts(rows)
    return {
        "setup": "{} · {} · {} runs".format(node_label, duration_label, len(rows)),
        "setup_key": (node_key, duration_key),
        "avg_total_bytes": statistics.mean(total_bytes_values) if total_bytes_values else 0.0,
        "avg_total_mb": statistics.mean(total_mb_values) if total_mb_values else 0.0,
        "avg_failures": statistics.mean(failure_values) if failure_values else 0.0,
        "avg_detection_speed": statistics.mean(detection_values) if detection_values else None,
    }


def _build_protocol_comparison_rows():
    suites_by_protocol = _latest_protocol_suite_index()
    signatures = set()
    for protocol_suites in suites_by_protocol.values():
        signatures.update(protocol_suites.keys())

    rows = []
    for signature in sorted(signatures, key=_scenario_sort_key):
        phase_id, challenge = signature
        egess_entry = suites_by_protocol.get("egess", {}).get(signature)
        checkin_entry = suites_by_protocol.get("checkin", {}).get(signature)
        egess_metrics = _suite_comparison_metrics(egess_entry) if egess_entry else None
        checkin_metrics = _suite_comparison_metrics(checkin_entry) if checkin_entry else None

        if egess_metrics is None and checkin_metrics is None:
            continue

        status = "Fair"
        note = "Ready for paper comparison."
        if egess_metrics is None or checkin_metrics is None:
            status = "Missing"
            note = "One protocol has not produced this scenario yet."
        elif egess_metrics.get("setup_key") != checkin_metrics.get("setup_key"):
            status = "Mismatch"
            note = "Match node count and duration before using this row in the paper."
        elif egess_metrics.get("avg_detection_speed") is None or checkin_metrics.get("avg_detection_speed") is None:
            note = "Detection speed needs fresh history-enabled runs for both protocols."

        rows.append(
            {
                "scenario_label": _scenario_label(phase_id, challenge),
                "egess_setup": egess_metrics.get("setup", "n/a") if egess_metrics else "n/a",
                "egess_bytes": (
                    "{:,.0f} B ({:.3f} MB)".format(egess_metrics.get("avg_total_bytes", 0.0), egess_metrics.get("avg_total_mb", 0.0))
                    if egess_metrics
                    else "n/a"
                ),
                "egess_failures": "{:.2f} / run".format(egess_metrics.get("avg_failures", 0.0)) if egess_metrics else "n/a",
                "egess_detection_speed": (
                    "{:.2f}s".format(egess_metrics.get("avg_detection_speed"))
                    if egess_metrics and egess_metrics.get("avg_detection_speed") is not None
                    else "n/a"
                ),
                "checkin_setup": checkin_metrics.get("setup", "n/a") if checkin_metrics else "n/a",
                "checkin_bytes": (
                    "{:,.0f} B ({:.3f} MB)".format(checkin_metrics.get("avg_total_bytes", 0.0), checkin_metrics.get("avg_total_mb", 0.0))
                    if checkin_metrics
                    else "n/a"
                ),
                "checkin_failures": "{:.2f} / run".format(checkin_metrics.get("avg_failures", 0.0)) if checkin_metrics else "n/a",
                "checkin_detection_speed": (
                    "{:.2f}s".format(checkin_metrics.get("avg_detection_speed"))
                    if checkin_metrics and checkin_metrics.get("avg_detection_speed") is not None
                    else "n/a"
                ),
                "comparison_status": status,
                "comparison_note": note,
            }
        )
    return rows


def _render_comparison_panel(comparison_rows):
    if not comparison_rows:
        return (
            """<section class="panel">
<div class="panel-head">
  <h2>Comparison Between Protocols</h2>
  <p class="section-note">Run both protocols first, then this section will compare the latest matching scenario suites.</p>
</div>
<div class="empty">No protocol comparison rows are available yet.</div>
</section>""",
            "",
        )

    scenario_order = ["Baseline", "Tornado", "Fire", "Bomb", "Ghost Outage + Noise"]
    seen = []
    for label in scenario_order + [row.get("scenario_label", "") for row in comparison_rows]:
        text = str(label).strip()
        if text and text not in seen:
            seen.append(text)

    button_html = ['<button type="button" class="scenario-tab active" data-scenario-filter="ALL">All</button>']
    for label in seen:
        slug = label.lower().replace(" ", "-").replace("+", "plus")
        button_html.append(
            '<button type="button" class="scenario-tab" data-scenario-filter="{slug}">{label}</button>'.format(
                slug=escape(slug),
                label=escape(label),
            )
        )

    head = "".join("<th>{}</th>".format(escape(_field_label(field))) for field in COMPARISON_FIELDS)
    body_rows = []
    for row in comparison_rows:
        scenario = str(row.get("scenario_label", "")).strip()
        slug = scenario.lower().replace(" ", "-").replace("+", "plus")
        cells = []
        for field in COMPARISON_FIELDS:
            display = escape(_format_display_value(field, row.get(field, "")))
            badge_class = _badge_class(field, row.get(field, ""))
            if badge_class:
                display = '<span class="badge {}">{}</span>'.format(badge_class, display)
            cells.append('<td class="{}">{}</td>'.format(_cell_class(field, row.get(field, "")), display))
        body_rows.append(
            '<tr class="{classes}" data-scenario="{scenario}">{cells}</tr>'.format(
                classes=_row_class(row),
                scenario=escape(slug),
                cells="".join(cells),
            )
        )

    panel_html = """<section class="panel">
<div class="panel-head">
  <h2>Comparison Between Protocols</h2>
  <p class="section-note">Detection speed uses the first LOCAL watch-node signal after scenario start. Use rows marked Fair for paper-ready comparisons.</p>
</div>
<div class="scenario-tab-row">{buttons}</div>
<div class="table-wrap">
  <table id="comparison-table">
    <thead><tr>{head}</tr></thead>
    <tbody>{body}</tbody>
  </table>
</div>
</section>""".format(buttons="".join(button_html), head=head, body="".join(body_rows))

    script_html = """<script>
(function () {
  const buttons = Array.from(document.querySelectorAll('.scenario-tab'));
  const rows = Array.from(document.querySelectorAll('#comparison-table tbody tr'));
  if (!buttons.length || !rows.length) return;

  function applyFilter(filterValue) {
    rows.forEach((row) => {
      const scenario = row.getAttribute('data-scenario') || '';
      row.style.display = (filterValue === 'ALL' || scenario === filterValue) ? '' : 'none';
    });
    buttons.forEach((button) => {
      button.classList.toggle('active', button.getAttribute('data-scenario-filter') === filterValue);
    });
  }

  buttons.forEach((button) => {
    button.addEventListener('click', () => {
      applyFilter(button.getAttribute('data-scenario-filter') || 'ALL');
    });
  });

  applyFilter('ALL');
})();
</script>"""
    return panel_html, script_html


def _render_suite_interactive_panel(summary_rows):
    if not summary_rows:
        return "", ""

    options_html = "".join(
        '<option value="{value}">{label}</option>'.format(value=escape(field), label=escape(_field_label(field)))
        for field in SUMMARY_CHART_FIELDS
    )
    summary_json = escape(json.dumps(summary_rows))
    labels_json = escape(json.dumps({field: _field_label(field) for field in SUMMARY_CHART_FIELDS}))

    panel_html = """<section class="panel">
<div class="panel-head">
  <h2>Flexible Averages</h2>
  <p class="section-note">Type any run count like 5, 14, or 23 to recompute prefix averages without rerunning the suite.</p>
</div>
<div class="control-row">
  <div class="control-field">
    <label for="suite-window-input">Runs To Include</label>
    <input id="suite-window-input" type="number" min="1" step="1">
  </div>
  <div class="control-field">
    <label for="suite-metric-select">Metric To Graph</label>
    <select id="suite-metric-select">{}</select>
  </div>
</div>
<div id="suite-window-cards" class="micro-grid"></div>
<div class="spotlight-grid" style="margin-top:12px;">
  <div class="spotlight-card">
    <h3>Selected Metric Trend</h3>
    <div id="suite-window-chart"></div>
    <p id="suite-window-note" class="micro-note"></p>
  </div>
  <div class="spotlight-card">
    <h3>Prefix Average Summary</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Metric</th><th>Value</th></tr></thead>
        <tbody id="suite-window-table"></tbody>
      </table>
    </div>
  </div>
</div>
</section>""".format(options_html)

    script_html = """<script type="application/json" id="suite-summary-data">{summary_json}</script>
<script type="application/json" id="suite-field-labels">{labels_json}</script>
<script>
(() => {{
  const rows = JSON.parse(document.getElementById('suite-summary-data').textContent || '[]');
  const labels = JSON.parse(document.getElementById('suite-field-labels').textContent || '{{}}');
  if (!rows.length) return;
  const input = document.getElementById('suite-window-input');
  const metricSelect = document.getElementById('suite-metric-select');
  const cardsHost = document.getElementById('suite-window-cards');
  const chartHost = document.getElementById('suite-window-chart');
  const tableHost = document.getElementById('suite-window-table');
  const noteHost = document.getElementById('suite-window-note');
  const intFmt = new Intl.NumberFormat('en-US');

  function toNum(value) {{
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }}

  function avg(field, subset) {{
    const values = subset.map(row => toNum(row[field])).filter(value => value !== null);
    if (!values.length) return null;
    return values.reduce((acc, value) => acc + value, 0) / values.length;
  }}

  function fmt(field, value) {{
    if (value === null || value === undefined || Number.isNaN(value)) return 'n/a';
    if (field === 'active_duration_sec' || field === 'duration_sec') return value.toFixed(3) + 's';
    if (field.endsWith('_mb')) return value.toFixed(3) + ' MB';
    if (field.includes('bytes')) return intFmt.format(Math.round(value));
    if (Math.abs(value - Math.round(value)) < 1e-9) return intFmt.format(Math.round(value));
    return value.toFixed(3);
  }}

  function lineSvg(points, color) {{
    if (!points.length) return '<div class="chart-empty">No data</div>';
    const width = 360;
    const height = 150;
    const padX = 14;
    const padTop = 14;
    const padBottom = 22;
    const values = points.map(point => point.value);
    let min = Math.min(...values);
    let max = Math.max(...values);
    if (Math.abs(max - min) < 1e-9) max = min + 1;
    const usableW = width - padX * 2;
    const usableH = height - padTop - padBottom - 10;
    const stepX = points.length > 1 ? usableW / (points.length - 1) : 0;
    const coords = points.map((point, idx) => {{
      const x = points.length > 1 ? padX + idx * stepX : padX + usableW / 2;
      const ratio = (point.value - min) / (max - min);
      const y = padTop + (1 - ratio) * usableH;
      return {{ x, y }};
    }});
    const polyline = coords.map(point => `${{point.x.toFixed(2)}},${{point.y.toFixed(2)}}`).join(' ');
    const fillPoly = [`${{coords[0].x.toFixed(2)}},${{(height - padBottom).toFixed(2)}}`]
      .concat(coords.map(point => `${{point.x.toFixed(2)}},${{point.y.toFixed(2)}}`))
      .concat([`${{coords[coords.length - 1].x.toFixed(2)}},${{(height - padBottom).toFixed(2)}}`])
      .join(' ');
    const circles = coords.map(point => `<circle cx="${{point.x.toFixed(2)}}" cy="${{point.y.toFixed(2)}}" r="2.8" fill="${{color}}"></circle>`).join('');
    return `<svg viewBox="0 0 ${{width}} ${{height}}" class="metric-chart" role="img" aria-label="metric chart">
      <line x1="${{padX}}" y1="${{height - padBottom}}" x2="${{width - padX}}" y2="${{height - padBottom}}" class="chart-axis"></line>
      <line x1="${{padX}}" y1="${{padTop}}" x2="${{padX}}" y2="${{height - padBottom}}" class="chart-axis"></line>
      <polygon points="${{fillPoly}}" fill="${{color}}" opacity="0.12"></polygon>
      <polyline points="${{polyline}}" fill="none" stroke="${{color}}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>
      ${{circles}}
    </svg>`;
  }}

  function render() {{
    const maxRuns = rows.length;
    let count = parseInt(input.value || maxRuns, 10);
    if (!Number.isFinite(count)) count = maxRuns;
    count = Math.max(1, Math.min(maxRuns, count));
    input.value = count;
    input.max = maxRuns;
    const subset = rows.slice(0, count);
    const metric = metricSelect.value || 'total_mb';
    const selectedAvg = avg(metric, subset);
    const selectedLatest = toNum(subset[subset.length - 1][metric]);
    const summaryCards = [
      {{ label: 'Runs Used', value: String(count), note: `of ${{maxRuns}} completed runs` }},
      {{ label: 'Avg Traffic', value: fmt('total_mb', avg('total_mb', subset)), note: 'selected prefix average' }},
      {{ label: 'Avg Failures', value: fmt('tx_fail_total', avg('tx_fail_total', subset)), note: 'TX failures per run' }},
      {{ label: 'Avg Reachable', value: fmt('reachable_nodes', avg('reachable_nodes', subset)), note: 'reachable nodes per run' }},
      {{ label: `Avg ${{labels[metric] || metric}}`, value: fmt(metric, selectedAvg), note: 'current selected metric' }},
    ];
    cardsHost.innerHTML = summaryCards.map(card => `<div class="micro-card"><div class="micro-label">${{card.label}}</div><div class="micro-value">${{card.value}}</div><div class="micro-note">${{card.note}}</div></div>`).join('');

    const points = subset
      .map(row => ({{ label: `R${{row.run_index}} @ N${{row.nodes}}`, value: toNum(row[metric]) }}))
      .filter(point => point.value !== null);
    chartHost.innerHTML = lineSvg(points, '#2474e5');
    noteHost.textContent = points.length
      ? `Showing the first ${{count}} runs. This is useful for checking early averages before all 30 runs finish.`
      : 'No numeric data for the selected metric.';

    const tableRows = [
      ['Average Traffic', fmt('total_mb', avg('total_mb', subset))],
      ['Average Failures', fmt('tx_fail_total', avg('tx_fail_total', subset))],
      ['Average Timeouts', fmt('tx_timeout_total', avg('tx_timeout_total', subset))],
      ['Average Reachable Nodes', fmt('reachable_nodes', avg('reachable_nodes', subset))],
      [`Average ${{labels[metric] || metric}}`, fmt(metric, selectedAvg)],
      [`Latest ${{labels[metric] || metric}}`, fmt(metric, selectedLatest)],
    ];
    tableHost.innerHTML = tableRows.map(([label, value]) => `<tr><td>${{label}}</td><td>${{value}}</td></tr>`).join('');
  }}

  input.value = rows.length;
  input.addEventListener('input', render);
  metricSelect.addEventListener('change', render);
  render();
}})();
</script>""".format(summary_json=summary_json, labels_json=labels_json)
    return panel_html, script_html


def _render_node_spotlight_panel(evidence, history_rows):
    node_payload = _node_spotlight_payload(evidence)
    if not node_payload:
        return "", ""

    options_html = "".join(
        '<option value="{value}">{label}</option>'.format(value=escape(str(row["port"])), label=escape(str(row["port"])))
        for row in node_payload
    )
    metric_options_html = "".join(
        '<option value="{value}">{label}</option>'.format(value=escape(field), label=escape(_field_label(field)))
        for field in WATCH_CHART_FIELDS + ["current_missing_count"]
    )
    node_json = escape(json.dumps(node_payload))
    history_json = escape(json.dumps(history_rows or []))

    panel_html = """<section class="panel">
<div class="panel-head">
  <h2>Node Spotlight</h2>
  <p class="section-note">Pick any node to inspect pulls, requests, state, and recent messages.</p>
</div>
<div class="control-row">
  <div class="control-field">
    <label for="spotlight-port-select">Node Port</label>
    <select id="spotlight-port-select">{}</select>
  </div>
  <div class="control-field">
    <label for="spotlight-metric-select">History Metric</label>
    <select id="spotlight-metric-select">{}</select>
  </div>
</div>
<div id="spotlight-cards" class="micro-grid"></div>
<div class="spotlight-grid" style="margin-top:12px;">
  <div class="spotlight-card">
    <h3>Selected Node History</h3>
    <div id="spotlight-chart"></div>
    <p id="spotlight-history-note" class="micro-note"></p>
  </div>
  <div class="spotlight-card">
    <h3>Recent Messages</h3>
    <ul id="spotlight-log" class="spotlight-log"></ul>
  </div>
</div>
<div class="spotlight-grid" style="margin-top:12px;">
  <div class="spotlight-card">
    <h3>Extra Context</h3>
    <div id="spotlight-extra"></div>
  </div>
</div>
</section>""".format(options_html, metric_options_html)

    script_html = """<script type="application/json" id="spotlight-node-data">{node_json}</script>
<script type="application/json" id="spotlight-history-data">{history_json}</script>
<script>
(() => {{
  const nodes = JSON.parse(document.getElementById('spotlight-node-data').textContent || '[]');
  const historyRows = JSON.parse(document.getElementById('spotlight-history-data').textContent || '[]');
  if (!nodes.length) return;
  const portSelect = document.getElementById('spotlight-port-select');
  const metricSelect = document.getElementById('spotlight-metric-select');
  const cardsHost = document.getElementById('spotlight-cards');
  const chartHost = document.getElementById('spotlight-chart');
  const noteHost = document.getElementById('spotlight-history-note');
  const logHost = document.getElementById('spotlight-log');
  const extraHost = document.getElementById('spotlight-extra');
  const intFmt = new Intl.NumberFormat('en-US');

  function toNum(value) {{
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }}

  function fmt(field, value) {{
    if (value === null || value === undefined || Number.isNaN(value)) return 'n/a';
    if (field.endsWith('_mb')) return value.toFixed(3) + ' MB';
    if (field.includes('bytes')) return intFmt.format(Math.round(value));
    if (Math.abs(value - Math.round(value)) < 1e-9) return intFmt.format(Math.round(value));
    return value.toFixed(3);
  }}

  function lineSvg(points, color) {{
    if (!points.length) return '<div class="chart-empty">No sampled history for this node in the current run.</div>';
    const width = 360;
    const height = 150;
    const padX = 14;
    const padTop = 14;
    const padBottom = 22;
    const values = points.map(point => point.value);
    let min = Math.min(...values);
    let max = Math.max(...values);
    if (Math.abs(max - min) < 1e-9) max = min + 1;
    const usableW = width - padX * 2;
    const usableH = height - padTop - padBottom - 10;
    const stepX = points.length > 1 ? usableW / (points.length - 1) : 0;
    const coords = points.map((point, idx) => {{
      const x = points.length > 1 ? padX + idx * stepX : padX + usableW / 2;
      const ratio = (point.value - min) / (max - min);
      const y = padTop + (1 - ratio) * usableH;
      return {{ x, y }};
    }});
    const polyline = coords.map(point => `${{point.x.toFixed(2)}},${{point.y.toFixed(2)}}`).join(' ');
    const fillPoly = [`${{coords[0].x.toFixed(2)}},${{(height - padBottom).toFixed(2)}}`]
      .concat(coords.map(point => `${{point.x.toFixed(2)}},${{point.y.toFixed(2)}}`))
      .concat([`${{coords[coords.length - 1].x.toFixed(2)}},${{(height - padBottom).toFixed(2)}}`])
      .join(' ');
    const circles = coords.map(point => `<circle cx="${{point.x.toFixed(2)}}" cy="${{point.y.toFixed(2)}}" r="2.8" fill="${{color}}"></circle>`).join('');
    return `<svg viewBox="0 0 ${{width}} ${{height}}" class="metric-chart" role="img" aria-label="metric chart">
      <line x1="${{padX}}" y1="${{height - padBottom}}" x2="${{width - padX}}" y2="${{height - padBottom}}" class="chart-axis"></line>
      <line x1="${{padX}}" y1="${{padTop}}" x2="${{padX}}" y2="${{height - padBottom}}" class="chart-axis"></line>
      <polygon points="${{fillPoly}}" fill="${{color}}" opacity="0.12"></polygon>
      <polyline points="${{polyline}}" fill="none" stroke="${{color}}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>
      ${{circles}}
    </svg>`;
  }}

  function render() {{
    const port = String(portSelect.value || nodes[0].port);
    const metric = metricSelect.value || 'pull_rx';
    const node = nodes.find(item => String(item.port) === port) || nodes[0];
    const history = historyRows
      .filter(item => String(item.port) === port)
      .map(item => ({{ label: String(item.sample_label || ''), value: toNum(item[metric]) }}))
      .filter(item => item.value !== null);

    const cards = [
      {{ label: 'State', value: node.protocol_state || 'UNKNOWN', note: node.boundary_kind || 'no boundary label' }},
      {{ label: 'Pull RX', value: fmt('pull_rx', toNum(node.pull_rx)), note: 'inbound pull requests served' }},
      {{ label: 'Pull TX', value: fmt('pull_tx', toNum(node.pull_tx)), note: 'outbound pull requests sent' }},
      {{ label: 'Push RX', value: fmt('push_rx', toNum(node.push_rx)), note: 'protocol push messages received' }},
      {{ label: 'Push TX', value: fmt('push_tx', toNum(node.push_tx)), note: 'protocol push messages sent' }},
      {{ label: 'Traffic', value: fmt('total_mb', toNum(node.total_mb)), note: fmt('total_bytes', toNum(node.total_bytes)) + ' total bytes' }},
      {{ label: 'Accepted Msgs', value: fmt('accepted_messages', toNum(node.accepted_messages)), note: 'messages accepted by the node' }},
      {{ label: 'Missing Neighbors', value: fmt('current_missing_count', toNum(node.current_missing_count)), note: 'current missing-neighbor count' }},
    ];
    cardsHost.innerHTML = cards.map(card => `<div class="micro-card"><div class="micro-label">${{card.label}}</div><div class="micro-value">${{card.value}}</div><div class="micro-note">${{card.note}}</div></div>`).join('');

    chartHost.innerHTML = lineSvg(history, '#118a7e');
    noteHost.textContent = history.length
      ? `Tracking ${{metric.replaceAll('_', ' ')}} for port ${{port}} across sampled pull history.`
      : 'No sampled history exists for this node in this run. New runs after the history patch will fill this in.';

    const recentMsgs = Array.isArray(node.recent_msgs) && node.recent_msgs.length ? node.recent_msgs : ['No recent messages captured.'];
    logHost.innerHTML = recentMsgs.map(item => `<li>${{item}}</li>`).join('');
    extraHost.innerHTML = `
      <div class="micro-grid">
        <div class="micro-card"><div class="micro-label">Phase</div><div class="micro-value">${{node.phase || 'n/a'}}</div><div class="micro-note">layer-2 phase or check-in phase</div></div>
        <div class="micro-card"><div class="micro-label">Direction</div><div class="micro-value">${{node.direction_label || 'n/a'}}</div><div class="micro-note">direction label when available</div></div>
        <div class="micro-card"><div class="micro-label">Pull Cycles</div><div class="micro-value">${{fmt('pull_cycles', toNum(node.pull_cycles))}}</div><div class="micro-note">local pull loop iterations</div></div>
        <div class="micro-card"><div class="micro-label">Known Nodes</div><div class="micro-value">${{fmt('known_nodes_count', toNum(node.known_nodes_count))}}</div><div class="micro-note">known-neighbor count</div></div>
        <div class="micro-card"><div class="micro-label">Incoming Events</div><div class="micro-value">${{fmt('incoming_events_count', toNum(node.incoming_events_count))}}</div><div class="micro-note">recent inbound event count</div></div>
        <div class="micro-card"><div class="micro-label">Fault Flags</div><div class="micro-value">${{node.crash_sim || node.lie_sensor || node.flap ? 'ACTIVE' : 'OFF'}}</div><div class="micro-note">crash=${{node.crash_sim ? 'on' : 'off'}}, lie=${{node.lie_sensor ? 'on' : 'off'}}, flap=${{node.flap ? 'on' : 'off'}}</div></div>
      </div>
      <p class="micro-note" style="margin-top:12px;">Recent alerts: ${{Array.isArray(node.recent_alerts) && node.recent_alerts.length ? node.recent_alerts.join(', ') : 'none captured'}}</p>
    `;
  }}

  portSelect.addEventListener('change', render);
  metricSelect.addEventListener('change', render);
  render();
}})();
</script>""".format(node_json=node_json, history_json=history_json)
    return panel_html, script_html


def _write_run_html(run_dir, manifest, summary_row, watch_rows, evidence, events_path, history_rows=None, history_totals_rows=None):
    history_rows = history_rows or []
    history_totals_rows = history_totals_rows or []
    node_rows = _all_node_rows(evidence)
    watch_ports = manifest.get("watch_ports", {})
    local_port = int(watch_ports.get("LOCAL", 0)) if watch_ports.get("LOCAL") is not None else None
    far_port = int(watch_ports.get("FAR", 0)) if watch_ports.get("FAR") is not None else None
    local_history = [row for row in history_rows if local_port is not None and _to_int(row.get("port", -1), -1) == int(local_port)]
    far_history = [row for row in history_rows if far_port is not None and _to_int(row.get("port", -1), -1) == int(far_port)]
    spotlight_html, spotlight_script = _render_node_spotlight_panel(evidence, history_rows)

    cards = [
        {
            "label": "Status",
            "value": _format_display_value("status", summary_row.get("status", "")),
            "note": "{} nodes".format(_format_display_value("nodes", summary_row.get("nodes", ""))),
            "tone": "good" if str(summary_row.get("status", "")).strip().lower() == "ok" else "bad",
        },
        {
            "label": "Traffic",
            "value": _format_display_value("total_mb", summary_row.get("total_mb", 0.0)),
            "note": "{} total bytes".format(_format_display_value("total_bytes", summary_row.get("total_bytes", 0))),
            "tone": "accent",
        },
        {
            "label": "Events",
            "value": _format_display_value("events_total", summary_row.get("events_total", 0)),
            "note": "{} trigger ops".format(_format_display_value("trigger_ops", summary_row.get("trigger_ops", 0))),
            "tone": "accent",
        },
        {
            "label": "Failures",
            "value": _format_display_value("tx_fail_total", summary_row.get("tx_fail_total", 0)),
            "note": "{} timeouts, {} conn errors".format(
                _format_display_value("tx_timeout_total", summary_row.get("tx_timeout_total", 0)),
                _format_display_value("tx_conn_error_total", summary_row.get("tx_conn_error_total", 0)),
            ),
            "tone": "bad" if _maybe_float(summary_row.get("tx_fail_total", 0)) else "good",
        },
    ]

    sections = [
        _render_glossary_html(),
        _render_table_html("Run Overview", [summary_row], RUN_OVERVIEW_FIELDS, "Color badges make the health signals easier to spot at a glance."),
        _render_table_html("Watched Nodes", watch_rows, WATCH_OVERVIEW_FIELDS, "Local rows are warm-toned, far rows are cool-toned."),
        spotlight_html,
        _render_chart_grid_html(
            "Pull History",
            history_totals_rows,
            ["pull_rx_total", "pull_tx_total", "push_rx_total", "push_tx_total", "accepted_messages_total", "total_mb"],
            _sample_label,
            "These sampled totals show how traffic and accepted messages evolve during the run.",
        ),
        _render_chart_grid_html(
            "Local Watch History",
            local_history,
            WATCH_CHART_FIELDS,
            _sample_label,
            "This follows the local watch node over time.",
        ),
        _render_chart_grid_html(
            "Far Watch History",
            far_history,
            WATCH_CHART_FIELDS,
            _sample_label,
            "This follows the far watch node over time.",
        ),
        _render_table_html("All Nodes Snapshot", node_rows, NODE_FIELDS, "Every node at the end of the run, so you can spot hotspots and outliers quickly."),
        _render_links_html(
            "Raw Files",
            [
                ("paper_summary.tsv", "paper_summary.tsv"),
                ("paper_watch_nodes.tsv", "paper_watch_nodes.tsv"),
                ("paper_all_nodes.tsv", "paper_all_nodes.tsv"),
                ("paper_pull_history.tsv", "paper_pull_history.tsv"),
                ("paper_pull_totals.tsv", "paper_pull_totals.tsv"),
                ("paper_summary.md", "paper_summary.md"),
                ("paper_manifest.json", "paper_manifest.json"),
                ("paper_evidence.json", "paper_evidence.json"),
                (Path(events_path).name, Path(events_path).name),
            ],
        ),
        "<details><summary>Show Pull Totals Table</summary>{}</details>".format(
            _render_table_html("Pull Totals Over Time", history_totals_rows, HISTORY_TOTAL_FIELDS)
        ),
        "<details><summary>Show Pull History Table</summary>{}</details>".format(
            _render_table_html("All Sampled Pull Rows", history_rows, HISTORY_FIELDS)
        ),
        "<details><summary>Show Full Summary Row</summary>{}</details>".format(
            _render_table_html("Full Summary Fields", [summary_row], SUMMARY_FIELDS)
        ),
        "<details><summary>Show Full Watch Rows</summary>{}</details>".format(
            _render_table_html("Full Watch Fields", watch_rows, WATCH_FIELDS)
        ),
    ]

    subtitle = "{} | {} | Run {} Seed {}".format(
        manifest.get("phase_name", "Paper Evaluation Run"),
        summary_row.get("challenge", ""),
        summary_row.get("run_index", ""),
        summary_row.get("seed", ""),
    )
    _write_text(run_dir / "paper_summary.html", _html_page("Paper Evaluation Run", subtitle, _render_cards_html(cards), "".join(sections), script_html=spotlight_script))


def _write_suite_html(report_dir, spec, summary_rows, watch_rows, summary_by_nodes_rows):
    total_runs = len(summary_rows)
    total_mb_values = [float(row.get("total_mb", 0.0)) for row in summary_rows]
    total_failures = sum(int(row.get("tx_fail_total", 0)) for row in summary_rows)
    ok_runs = sum(1 for row in summary_rows if str(row.get("status", "")).strip().lower() == "ok")
    summary_metric_rows = _metric_summary_rows(summary_rows, SUMMARY_CHART_FIELDS)
    interactive_html, interactive_script = _render_suite_interactive_panel(summary_rows)
    comparison_rows = _build_protocol_comparison_rows()
    comparison_html, comparison_script = _render_comparison_panel(comparison_rows)
    cards = [
        {
            "label": "Protocol",
            "value": str(spec.get("protocol", "")).upper(),
            "note": str(spec.get("phase_name", "")),
            "tone": "accent",
        },
        {
            "label": "Runs",
            "value": str(total_runs),
            "note": "{} node settings".format(len({int(row.get("nodes", 0)) for row in summary_rows})),
            "tone": "accent",
        },
        {
            "label": "Avg Traffic",
            "value": "{:.3f} MB".format(statistics.mean(total_mb_values)) if total_mb_values else "0.000 MB",
            "note": "per run",
            "tone": "accent",
        },
        {
            "label": "Healthy Runs",
            "value": "{}/{}".format(ok_runs, total_runs),
            "note": "{} TX failures".format(total_failures),
            "tone": "good" if total_failures == 0 else "warn",
        },
    ]

    sections = [
        _render_glossary_html(),
        interactive_html,
        comparison_html,
        _render_run_deep_dive_html(report_dir, summary_rows),
        _render_table_html("Run Overview", summary_rows, RUN_OVERVIEW_FIELDS, "This is the quickest table to compare one run against another."),
        _render_chart_grid_html(
            "Run Metric Charts",
            summary_rows,
            SUMMARY_CHART_FIELDS,
            _run_label,
            "Each chart tracks one metric across the saved runs, which is useful when you do the full 30-run suite.",
        ),
        _render_table_html("Run Metric Averages", summary_metric_rows, ["metric", "samples", "avg", "min", "max", "latest"], "Quick average/min/max table for the suite metrics."),
        _render_table_html("Watched Nodes", watch_rows, WATCH_OVERVIEW_FIELDS, "Use this for per-node load, state, and fault visibility."),
        _render_table_html("Grouped By Node Count", summary_by_nodes_rows, SUMMARY_BY_NODES_FIELDS, "A compact roll-up for 49 vs 64 vs 81 style comparisons."),
        _render_links_html(
            "Raw Files",
            [
                ("all_runs.tsv", "all_runs.tsv"),
                ("all_watch_nodes.tsv", "all_watch_nodes.tsv"),
                ("summary_by_nodes.tsv", "summary_by_nodes.tsv"),
                ("metric_averages.tsv", "metric_averages.tsv"),
                ("protocol_comparison.tsv", "protocol_comparison.tsv"),
                ("README.md", "README.md"),
            ],
        ),
        "<details><summary>Show Full Run Table</summary>{}</details>".format(
            _render_table_html("All Run Fields", summary_rows, SUMMARY_FIELDS)
        ),
        "<details><summary>Show Full Watch Table</summary>{}</details>".format(
            _render_table_html("All Watch Fields", watch_rows, WATCH_FIELDS)
        ),
    ]

    subtitle = "{} | {} | {} second window".format(
        spec.get("phase_name", "Paper Evaluation Suite"),
        spec.get("challenge", ""),
        spec.get("duration_sec", ""),
    )
    _write_text(
        report_dir / "index.html",
        _html_page(
            "Paper Evaluation Dashboard",
            subtitle,
            _render_cards_html(cards),
            "".join(sections),
            script_html=(interactive_script + comparison_script),
        ),
    )


def _latest_run_dir():
    if not RUNS_DIR.exists():
        raise RuntimeError("runs directory does not exist yet")
    candidates = [path for path in RUNS_DIR.iterdir() if path.is_dir()]
    if not candidates:
        raise RuntimeError("no run directories exist yet")
    return sorted(candidates, key=lambda item: item.stat().st_mtime, reverse=True)[0]


def _stop_nodes():
    subprocess.run(["./stop_nodes.sh"], cwd=str(ROOT_DIR), check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _start_nodes(number_of_nodes):
    env = os.environ.copy()
    env["EGESS_LOG"] = "1"
    subprocess.run(
        ["./start_nodes.sh", str(int(number_of_nodes))],
        cwd=str(ROOT_DIR),
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return _latest_run_dir()


def _wait_until_ready(base_port, number_of_nodes, timeout_sec=35.0):
    deadline = time.monotonic() + float(timeout_sec)
    while time.monotonic() < deadline:
        ready = 0
        for port in range(int(base_port), int(base_port) + int(number_of_nodes)):
            try:
                res = _pull_state(port, origin="bootstrap", timeout=0.8)
                if isinstance(res, dict) and res.get("op") == "receipt":
                    ready += 1
            except Exception:
                pass
        if ready >= int(number_of_nodes):
            return True
        time.sleep(1.0)
    return False


def _tornado_sweep_batches(base_port, number_of_nodes, seed, width):
    grid = _auto_grid_size(number_of_nodes)
    width = max(1, min(int(width), int(grid)))
    rng = random.Random(int(seed))
    direction = rng.randint(0, 3)
    batches = []

    if direction in (0, 1):
        start_row = rng.randint(0, grid - width)
        band_rows = list(range(start_row, start_row + width))
        for sweep_idx in range(grid):
            col = sweep_idx if direction == 0 else (grid - 1 - sweep_idx)
            ports = []
            for row in band_rows:
                port = _rc_to_port(base_port, row, col, grid, number_of_nodes)
                if port is not None:
                    ports.append(int(port))
            if ports:
                batches.append(sorted(ports))
    else:
        start_col = rng.randint(0, grid - width)
        band_cols = list(range(start_col, start_col + width))
        for sweep_idx in range(grid):
            row = sweep_idx if direction == 2 else (grid - 1 - sweep_idx)
            ports = []
            for col in band_cols:
                port = _rc_to_port(base_port, row, col, grid, number_of_nodes)
                if port is not None:
                    ports.append(int(port))
            if ports:
                batches.append(sorted(ports))

    return batches


def _baseline_actions(spec, base_port, number_of_nodes, seed):
    del spec, base_port, number_of_nodes, seed
    return []


def _tornado_actions(spec, base_port, number_of_nodes, seed):
    duration_sec = _to_float(spec.get("duration_sec", 60), 60.0)
    width = _to_int(spec.get("scenario", {}).get("tornado_width", 2), 2)
    batches = _tornado_sweep_batches(base_port, number_of_nodes, seed, width)
    actions = []
    if len(batches) == 0:
        return actions

    baseline_gap = max(2.0, duration_sec * 0.10)
    sweep_window = max(4.0, duration_sec * 0.60)
    step_gap = sweep_window / max(1, len(batches) - 1) if len(batches) > 1 else sweep_window
    killed_ports = sorted({port for batch in batches for port in batch})

    for idx, ports in enumerate(batches):
        actions.append(
            {
                "at_sec": round(baseline_gap + (idx * step_gap), 3),
                "kind": "crash_batch",
                "ports": [int(port) for port in ports],
                "label": "tornado_step_{}".format(idx + 1),
            }
        )

    recovery_at = min(duration_sec - 1.0, max(baseline_gap + sweep_window + 1.0, duration_sec * 0.78))
    reset_at = min(duration_sec - 0.2, max(recovery_at + 1.0, duration_sec * 0.93))
    actions.append(
        {
            "at_sec": round(recovery_at, 3),
            "kind": "recover_batch",
            "ports": killed_ports,
            "label": "tornado_recovery",
        }
    )
    actions.append(
        {
            "at_sec": round(reset_at, 3),
            "kind": "reset_batch",
            "ports": killed_ports,
            "label": "tornado_reset",
        }
    )
    return actions


def _stress_actions(spec, base_port, number_of_nodes, seed):
    del seed
    duration_sec = _to_float(spec.get("duration_sec", 60), 60.0)
    period_sec = _to_int(spec.get("scenario", {}).get("fault_period_sec", 4), 4)
    target = _center_port(base_port, number_of_nodes)
    neighbors = _neighbors_for_port(base_port, number_of_nodes, target)
    lie_port = int(neighbors[0]) if len(neighbors) > 0 else int(target)
    flap_port = int(neighbors[1]) if len(neighbors) > 1 else int(lie_port)
    actions = [
        {
            "at_sec": round(duration_sec * 0.15, 3),
            "kind": "fault_toggle",
            "port": int(target),
            "fault": "crash_sim",
            "enable": True,
            "period_sec": period_sec,
            "label": "ghost_outage_on",
        },
        {
            "at_sec": round(duration_sec * 0.28, 3),
            "kind": "fault_toggle",
            "port": int(target),
            "fault": "crash_sim",
            "enable": False,
            "period_sec": period_sec,
            "label": "ghost_outage_off",
        },
        {
            "at_sec": round(duration_sec * 0.40, 3),
            "kind": "fault_toggle",
            "port": int(lie_port),
            "fault": "lie_sensor",
            "enable": True,
            "period_sec": period_sec,
            "label": "lie_sensor_on",
        },
        {
            "at_sec": round(duration_sec * 0.55, 3),
            "kind": "fault_toggle",
            "port": int(lie_port),
            "fault": "lie_sensor",
            "enable": False,
            "period_sec": period_sec,
            "label": "lie_sensor_off",
        },
        {
            "at_sec": round(duration_sec * 0.62, 3),
            "kind": "fault_toggle",
            "port": int(flap_port),
            "fault": "flap",
            "enable": True,
            "period_sec": max(2, period_sec),
            "label": "flap_on",
        },
        {
            "at_sec": round(duration_sec * 0.82, 3),
            "kind": "fault_toggle",
            "port": int(flap_port),
            "fault": "flap",
            "enable": False,
            "period_sec": max(2, period_sec),
            "label": "flap_off",
        },
        {
            "at_sec": round(duration_sec * 0.88, 3),
            "kind": "state_batch",
            "ports": [int(target), int(lie_port), int(flap_port)],
            "sensor_state": "RECOVERING",
            "label": "stress_recovering",
        },
        {
            "at_sec": round(duration_sec * 0.96, 3),
            "kind": "reset_batch",
            "ports": [int(target), int(lie_port), int(flap_port)],
            "label": "stress_reset",
        },
    ]
    return actions


def _scenario_actions(spec, base_port, number_of_nodes, seed):
    kind = str(spec.get("scenario", {}).get("kind", "baseline")).strip().lower()
    if kind == "baseline":
        return _baseline_actions(spec, base_port, number_of_nodes, seed)
    if kind == "tornado_sweep":
        return _tornado_actions(spec, base_port, number_of_nodes, seed)
    if kind == "ghost_outage_noise":
        return _stress_actions(spec, base_port, number_of_nodes, seed)
    raise ValueError("unsupported scenario kind: {}".format(kind))


def _watch_ports(spec, base_port, number_of_nodes, seed):
    kind = str(spec.get("scenario", {}).get("kind", "baseline")).strip().lower()
    if kind == "tornado_sweep":
        batches = _tornado_sweep_batches(base_port, number_of_nodes, seed, spec.get("scenario", {}).get("tornado_width", 2))
        local_watch = int(batches[0][0]) if len(batches) > 0 and len(batches[0]) > 0 else _center_port(base_port, number_of_nodes)
    elif kind == "ghost_outage_noise":
        local_watch = _center_port(base_port, number_of_nodes)
    else:
        local_watch = _center_port(base_port, number_of_nodes)

    far_watch = _farthest_port(base_port, number_of_nodes, local_watch)
    if int(far_watch) == int(local_watch) and int(number_of_nodes) > 1:
        far_watch = int(base_port) + int(number_of_nodes) - 1
    return {
        "LOCAL": int(local_watch),
        "FAR": int(far_watch),
    }


def _apply_action(action, events_path):
    kind = str(action.get("kind", ""))
    label = str(action.get("label", kind))

    if kind == "crash_batch":
        for port in action.get("ports", []):
            res = _inject_fault(port, "crash_sim", True, period_sec=action.get("period_sec", 4))
            _log_event(events_path, "fault", {"label": label, "port": int(port), "fault": "crash_sim", "enable": True, "response": res})
        return

    if kind == "recover_batch":
        for port in action.get("ports", []):
            res_fault = _inject_fault(port, "crash_sim", False, period_sec=action.get("period_sec", 4))
            res_state = _inject_state(port, "RECOVERING")
            _log_event(events_path, "fault", {"label": label, "port": int(port), "fault": "crash_sim", "enable": False, "response": res_fault})
            _log_event(events_path, "state", {"label": label, "port": int(port), "sensor_state": "RECOVERING", "response": res_state})
        return

    if kind == "reset_batch":
        for port in action.get("ports", []):
            res_reset = _inject_fault(port, "reset", True, period_sec=action.get("period_sec", 4))
            res_state = _inject_state(port, "NORMAL")
            _log_event(events_path, "fault", {"label": label, "port": int(port), "fault": "reset", "enable": True, "response": res_reset})
            _log_event(events_path, "state", {"label": label, "port": int(port), "sensor_state": "NORMAL", "response": res_state})
        return

    if kind == "fault_toggle":
        res = _inject_fault(
            action.get("port"),
            action.get("fault"),
            bool(action.get("enable", True)),
            period_sec=action.get("period_sec", 4),
        )
        _log_event(
            events_path,
            "fault",
            {
                "label": label,
                "port": int(action.get("port")),
                "fault": str(action.get("fault")),
                "enable": bool(action.get("enable", True)),
                "period_sec": int(action.get("period_sec", 4)),
                "response": res,
            },
        )
        return

    if kind == "state_batch":
        for port in action.get("ports", []):
            res = _inject_state(port, action.get("sensor_state", "NORMAL"))
            _log_event(events_path, "state", {"label": label, "port": int(port), "sensor_state": str(action.get("sensor_state", "NORMAL")), "response": res})
        return

    raise ValueError("unsupported action kind: {}".format(kind))


def _run_active_window(spec, base_port, number_of_nodes, run_index, seed, events_path, history_path=None, history_totals_path=None):
    duration_sec = _to_float(spec.get("duration_sec", 60), 60.0)
    trigger_interval_sec = max(0.25, _to_float(spec.get("trigger_interval_sec", 2), 2.0))
    sample_interval_sec = max(0.5, _to_float(spec.get("sample_interval_sec", 1.0), 1.0))
    ports = list(range(int(base_port), int(base_port) + int(number_of_nodes)))
    actions = sorted(_scenario_actions(spec, base_port, number_of_nodes, seed), key=lambda item: float(item.get("at_sec", 0.0)))

    start = time.monotonic()
    deadline = start + float(duration_sec)
    next_trigger = start
    next_sample = start
    trigger_index = 0
    sample_index = 0
    action_index = 0

    _log_event(events_path, "stage", {"name": "active_window_start", "duration_sec": duration_sec})

    while True:
        now = time.monotonic()
        if now >= deadline:
            break

        elapsed = now - start
        while action_index < len(actions) and elapsed >= float(actions[action_index].get("at_sec", 0.0)):
            _apply_action(actions[action_index], events_path)
            action_index += 1

        if now >= next_trigger:
            port = ports[trigger_index % len(ports)]
            label = "{}_run{}_idx{}".format(str(spec.get("challenge", "demo")), int(run_index), int(trigger_index))
            try:
                res = _trigger_push(port, label)
                ok = bool(res.get("data", {}).get("success", False))
                _log_event(events_path, "trigger", {"port": int(port), "label": label, "ok": ok, "response": res})
            except Exception as exc:
                _log_event(events_path, "trigger_error", {"port": int(port), "label": label, "error": str(exc)})
            trigger_index += 1
            next_trigger += float(trigger_interval_sec)

        if history_path is not None and history_totals_path is not None and now >= next_sample:
            history_rows, totals_row = _sample_nodes(base_port, number_of_nodes, sample_index, now - start)
            for row in history_rows:
                _append_jsonl(history_path, row)
            _append_jsonl(history_totals_path, totals_row)
            sample_index += 1
            next_sample += float(sample_interval_sec)

        remaining = max(0.0, deadline - time.monotonic())
        time.sleep(min(0.05, remaining if remaining > 0.0 else 0.0))

    active_duration_sec = round(float(time.monotonic() - start), 3)

    _log_event(events_path, "done", {"active_duration_sec": active_duration_sec, "trigger_count": int(trigger_index)})
    return active_duration_sec, int(trigger_index)


def _collect_evidence(spec, run_dir, events_path, base_port, number_of_nodes, run_index, seed, active_duration_sec):
    totals = {
        "pull_rx": 0,
        "push_rx": 0,
        "pull_tx": 0,
        "push_tx": 0,
        "pull_rx_bytes": 0,
        "push_rx_bytes": 0,
        "pull_tx_bytes": 0,
        "push_tx_bytes": 0,
        "rx_total_bytes": 0,
        "tx_total_bytes": 0,
        "tx_ok": 0,
        "tx_fail": 0,
        "tx_timeout": 0,
        "tx_conn_error": 0,
    }
    nodes = {}
    reachable = 0

    for port in range(int(base_port), int(base_port) + int(number_of_nodes)):
        try:
            res = _pull_state(port, origin="paper_report", timeout=1.0)
            state = res.get("data", {}).get("node_state", {}) if isinstance(res, dict) else {}
            counters = state.get("msg_counters", {})
            if not isinstance(counters, dict):
                counters = {}
            for key in totals:
                totals[key] += _to_int(counters.get(key, 0), 0)
            nodes[str(port)] = {
                "reachable": True,
                "state": state,
                "msg_counters": counters,
            }
            reachable += 1
        except Exception as exc:
            nodes[str(port)] = {
                "reachable": False,
                "error": str(exc),
            }

    event_counts = {
        "events_total": 0,
        "fault_ops": 0,
        "trigger_ops": 0,
        "state_ops": 0,
    }
    with open(events_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if len(line) == 0:
                continue
            event_counts["events_total"] += 1
            row = json.loads(line)
            kind = str(row.get("kind", ""))
            if kind in ("fault", "fault_error"):
                event_counts["fault_ops"] += 1
            elif kind in ("trigger", "trigger_error"):
                event_counts["trigger_ops"] += 1
            elif kind == "state":
                event_counts["state_ops"] += 1

    watch_ports = _watch_ports(spec, base_port, number_of_nodes, seed)
    watch_rows = []
    for view, port in watch_ports.items():
        node_info = nodes.get(str(port), {"reachable": False})
        state = node_info.get("state", {})
        counters = node_info.get("msg_counters", {})
        layer2 = state.get("layer2_confirmation", {}) if isinstance(state, dict) else {}
        faults = state.get("faults", {}) if isinstance(state, dict) else {}
        total_bytes = _to_int(counters.get("rx_total_bytes", 0), 0) + _to_int(counters.get("tx_total_bytes", 0), 0)
        watch_rows.append(
            {
                "suite_id": str(spec.get("suite_id", "")),
                "phase_id": str(spec.get("phase_id", "")),
                "phase_name": str(spec.get("phase_name", "")),
                "protocol": str(spec.get("protocol", "")),
                "challenge": str(spec.get("challenge", "")),
                "duration_sec": _to_int(spec.get("duration_sec", 60), 60),
                "nodes": int(number_of_nodes),
                "run_index": int(run_index),
                "seed": int(seed),
                "view": str(view),
                "watch_port": int(port),
                "reachable": bool(node_info.get("reachable", False)),
                "protocol_state": str(state.get("protocol_state", "")),
                "boundary_kind": str(state.get("boundary_kind", "")),
                "score": _to_float(state.get("score", 0.0), 0.0),
                "front_score": _to_float(state.get("front_score", 0.0), 0.0),
                "impact_score": _to_float(state.get("impact_score", 0.0), 0.0),
                "arrest_score": _to_float(state.get("arrest_score", 0.0), 0.0),
                "coherence_score": _to_int(state.get("coherence_score", 0), 0),
                "accepted_messages": _to_int(state.get("accepted_messages", 0), 0),
                "pull_rx": _to_int(counters.get("pull_rx", 0), 0),
                "push_rx": _to_int(counters.get("push_rx", 0), 0),
                "pull_tx": _to_int(counters.get("pull_tx", 0), 0),
                "push_tx": _to_int(counters.get("push_tx", 0), 0),
                "rx_total_bytes": _to_int(counters.get("rx_total_bytes", 0), 0),
                "tx_total_bytes": _to_int(counters.get("tx_total_bytes", 0), 0),
                "total_bytes": int(total_bytes),
                "total_mb": round(float(total_bytes) / 1048576.0, 3),
                "direction_label": str(layer2.get("direction_label", "")),
                "phase": str(layer2.get("phase", "")),
                "distance_hops": _to_float(layer2.get("distance_hops", 99.0), 99.0),
                "eta_cycles": _to_float(layer2.get("eta_cycles", 99.0), 99.0),
                "current_missing_count": len(state.get("current_missing_neighbors", [])) if isinstance(state.get("current_missing_neighbors"), list) else 0,
                "crash_sim": bool(faults.get("crash_sim", False)),
                "lie_sensor": bool(faults.get("lie_sensor", False)),
                "flap": bool(faults.get("flap", False)),
            }
        )

    total_bytes = int(totals["rx_total_bytes"]) + int(totals["tx_total_bytes"])
    summary_row = {
        "suite_id": str(spec.get("suite_id", "")),
        "phase_id": str(spec.get("phase_id", "")),
        "phase_name": str(spec.get("phase_name", "")),
        "protocol": str(spec.get("protocol", "")),
        "challenge": str(spec.get("challenge", "")),
        "duration_sec": _to_int(spec.get("duration_sec", 60), 60),
        "active_duration_sec": float(active_duration_sec),
        "nodes": int(number_of_nodes),
        "run_index": int(run_index),
        "seed": int(seed),
        "run_dir": str(run_dir.relative_to(ROOT_DIR)),
        "local_watch_port": int(watch_ports["LOCAL"]),
        "far_watch_port": int(watch_ports["FAR"]),
        "reachable_nodes": int(reachable),
        "total_nodes": int(number_of_nodes),
        "events_total": int(event_counts["events_total"]),
        "fault_ops": int(event_counts["fault_ops"]),
        "trigger_ops": int(event_counts["trigger_ops"]),
        "pull_rx_total": int(totals["pull_rx"]),
        "push_rx_total": int(totals["push_rx"]),
        "pull_tx_total": int(totals["pull_tx"]),
        "push_tx_total": int(totals["push_tx"]),
        "rx_bytes_total": int(totals["rx_total_bytes"]),
        "tx_bytes_total": int(totals["tx_total_bytes"]),
        "total_bytes": int(total_bytes),
        "total_mb": round(float(total_bytes) / 1048576.0, 3),
        "tx_ok_total": int(totals["tx_ok"]),
        "tx_fail_total": int(totals["tx_fail"]),
        "tx_timeout_total": int(totals["tx_timeout"]),
        "tx_conn_error_total": int(totals["tx_conn_error"]),
        "status": "OK" if int(reachable) == int(number_of_nodes) else "WARN",
    }

    manifest = {
        "suite_id": str(spec.get("suite_id", "")),
        "phase_id": str(spec.get("phase_id", "")),
        "phase_name": str(spec.get("phase_name", "")),
        "protocol": str(spec.get("protocol", "")),
        "challenge": str(spec.get("challenge", "")),
        "duration_sec": _to_int(spec.get("duration_sec", 60), 60),
        "active_duration_sec": float(active_duration_sec),
        "nodes": int(number_of_nodes),
        "run_index": int(run_index),
        "seed": int(seed),
        "watch_ports": watch_ports,
        "spec_path": str(spec.get("_spec_path", "")),
    }

    return manifest, summary_row, watch_rows, {"nodes": nodes, "totals": totals, "event_counts": event_counts}


def _write_run_reports(run_dir, manifest, summary_row, watch_rows, evidence, events_path, history_path=None, history_totals_path=None):
    manifest_path = run_dir / "paper_manifest.json"
    summary_tsv_path = run_dir / "paper_summary.tsv"
    watch_tsv_path = run_dir / "paper_watch_nodes.tsv"
    all_nodes_tsv_path = run_dir / "paper_all_nodes.tsv"
    history_tsv_path = run_dir / "paper_pull_history.tsv"
    history_totals_tsv_path = run_dir / "paper_pull_totals.tsv"
    evidence_path = run_dir / "paper_evidence.json"
    summary_md_path = run_dir / "paper_summary.md"
    history_rows = _load_jsonl(history_path) if history_path else []
    history_totals_rows = _load_jsonl(history_totals_path) if history_totals_path else []
    node_rows = _all_node_rows(evidence)

    _write_json(manifest_path, manifest)
    _write_json(evidence_path, evidence)
    _write_tsv(summary_tsv_path, [summary_row], SUMMARY_FIELDS)
    _write_tsv(watch_tsv_path, watch_rows, WATCH_FIELDS)
    _write_tsv(all_nodes_tsv_path, node_rows, NODE_FIELDS)
    _write_tsv(history_tsv_path, history_rows, HISTORY_FIELDS)
    _write_tsv(history_totals_tsv_path, history_totals_rows, HISTORY_TOTAL_FIELDS)
    _write_run_html(run_dir, manifest, summary_row, watch_rows, evidence, events_path, history_rows=history_rows, history_totals_rows=history_totals_rows)

    lines = [
        "# {}".format(manifest.get("phase_name", "Paper Evaluation Run")),
        "",
        "- Phase: `{}`".format(manifest.get("phase_id", "")),
        "- Challenge: `{}`".format(summary_row.get("challenge", "")),
        "- Nodes: `{}`".format(summary_row.get("nodes", "")),
        "- Duration (requested / active): `{}` / `{}` seconds".format(summary_row.get("duration_sec", ""), summary_row.get("active_duration_sec", "")),
        "- Run index / seed: `{}` / `{}`".format(summary_row.get("run_index", ""), summary_row.get("seed", "")),
        "- Local / Far watch ports: `{}` / `{}`".format(summary_row.get("local_watch_port", ""), summary_row.get("far_watch_port", "")),
        "- Reachable nodes: `{}` / `{}`".format(summary_row.get("reachable_nodes", ""), summary_row.get("total_nodes", "")),
        "- Message totals (pull rx, push rx, pull tx, push tx): `{}`, `{}`, `{}`, `{}`".format(
            summary_row.get("pull_rx_total", ""),
            summary_row.get("push_rx_total", ""),
            summary_row.get("pull_tx_total", ""),
            summary_row.get("push_tx_total", ""),
        ),
        "- Byte totals (rx, tx, combined MB): `{}`, `{}`, `{}`".format(
            summary_row.get("rx_bytes_total", ""),
            summary_row.get("tx_bytes_total", ""),
            summary_row.get("total_mb", ""),
        ),
        "- Failure totals (fail / timeout / conn_error): `{}` / `{}` / `{}`".format(
            summary_row.get("tx_fail_total", ""),
            summary_row.get("tx_timeout_total", ""),
            summary_row.get("tx_conn_error_total", ""),
        ),
        "- Visual dashboard: `paper_summary.html`",
        "- Events JSONL: `{}`".format(str(Path(events_path).name)),
        "- Summary TSV: `{}`".format(summary_tsv_path.name),
        "- Watch TSV: `{}`".format(watch_tsv_path.name),
        "- All-node TSV: `{}`".format(all_nodes_tsv_path.name),
        "- Pull-history TSV: `{}`".format(history_tsv_path.name),
        "- Pull-totals TSV: `{}`".format(history_totals_tsv_path.name),
    ]
    with open(summary_md_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def _suite_case_rows(spec, max_runs=None, node_counts_override=None):
    node_counts = spec.get("node_counts", [])
    if node_counts_override:
        node_counts = [int(value) for value in node_counts_override]
    run_count = _to_int(spec.get("run_count", 1), 1)
    if max_runs is not None:
        run_count = min(int(run_count), int(max_runs))
    base_seed = _to_int(spec.get("seed_base", 1000), 1000)
    rows = []
    for node_count in node_counts:
        for run_index in range(1, int(run_count) + 1):
            rows.append(
                {
                    "nodes": int(node_count),
                    "run_index": int(run_index),
                    "seed": int(base_seed + run_index - 1),
                }
            )
    return rows


def _suite_summary_rows(summary_rows):
    groups = {}
    for row in summary_rows:
        key = (row["phase_id"], row["challenge"], row["duration_sec"], row["nodes"])
        groups.setdefault(key, []).append(row)

    out = []
    for key, rows in sorted(groups.items()):
        totals_mb = [float(item.get("total_mb", 0.0)) for item in rows]
        push_rx_total = [int(item.get("push_rx_total", 0)) for item in rows]
        tx_fail_total = [int(item.get("tx_fail_total", 0)) for item in rows]
        out.append(
            {
                "phase_id": key[0],
                "challenge": key[1],
                "duration_sec": key[2],
                "nodes": key[3],
                "runs": len(rows),
                "avg_total_mb": round(statistics.mean(totals_mb), 3) if totals_mb else 0.0,
                "avg_push_rx_total": round(statistics.mean(push_rx_total), 3) if push_rx_total else 0.0,
                "avg_tx_fail_total": round(statistics.mean(tx_fail_total), 3) if tx_fail_total else 0.0,
            }
        )
    return out


def _write_suite_reports(report_dir, spec, summary_rows, watch_rows):
    all_runs_tsv = report_dir / "all_runs.tsv"
    all_watch_tsv = report_dir / "all_watch_nodes.tsv"
    summary_by_nodes_tsv = report_dir / "summary_by_nodes.tsv"
    metric_averages_tsv = report_dir / "metric_averages.tsv"
    comparison_tsv = report_dir / "protocol_comparison.tsv"
    summary_md = report_dir / "README.md"

    _write_tsv(all_runs_tsv, summary_rows, SUMMARY_FIELDS)
    _write_tsv(all_watch_tsv, watch_rows, WATCH_FIELDS)

    summary_by_nodes_rows = _suite_summary_rows(summary_rows)
    comparison_rows = _build_protocol_comparison_rows()
    _write_tsv(summary_by_nodes_tsv, summary_by_nodes_rows, SUMMARY_BY_NODES_FIELDS)
    _write_tsv(metric_averages_tsv, _metric_summary_rows(summary_rows, SUMMARY_CHART_FIELDS), ["metric", "samples", "avg", "min", "max", "latest"])
    _write_tsv(comparison_tsv, comparison_rows, COMPARISON_FIELDS)
    _write_suite_html(report_dir, spec, summary_rows, watch_rows, summary_by_nodes_rows)

    lines = [
        "# {}".format(spec.get("phase_name", "Paper Evaluation Suite")),
        "",
        "- Suite ID: `{}`".format(spec.get("suite_id", "")),
        "- Protocol: `{}`".format(spec.get("protocol", "")),
        "- Challenge: `{}`".format(spec.get("challenge", "")),
        "- Runs completed: `{}`".format(len(summary_rows)),
        "- Visual dashboard: `index.html`",
        "- Aggregate run table: `all_runs.tsv`",
        "- Watch-node table: `all_watch_nodes.tsv`",
        "- Grouped summary: `summary_by_nodes.tsv`",
        "- Metric averages: `metric_averages.tsv`",
        "- Protocol comparison: `protocol_comparison.tsv`",
    ]
    with open(summary_md, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def _load_spec(path):
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    payload["_spec_path"] = str(path)
    return payload


def _validate_spec(spec):
    if str(spec.get("protocol", "")).strip().lower() != "egess":
        raise ValueError("only protocol='egess' is supported by this runner right now")
    if not isinstance(spec.get("node_counts", []), list) or len(spec.get("node_counts", [])) == 0:
        raise ValueError("spec must define a non-empty node_counts list")
    if _to_int(spec.get("run_count", 0), 0) < 1:
        raise ValueError("spec run_count must be >= 1")
    if _to_int(spec.get("duration_sec", 0), 0) < 1:
        raise ValueError("spec duration_sec must be >= 1")
    if str(spec.get("phase_id", "")).strip() == "":
        raise ValueError("spec phase_id is required")
    if str(spec.get("suite_id", "")).strip() == "":
        raise ValueError("spec suite_id is required")


def _report_dir_for_spec(spec):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    report_dir = REPORTS_DIR / "{}_{}".format(str(spec.get("suite_id", "suite")), stamp)
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir


def _run_case(spec_for_run, case):
    base_port = _to_int(spec_for_run.get("base_port", 9000), 9000)
    number_of_nodes = int(case["nodes"])
    run_index = int(case["run_index"])
    seed = int(case["seed"])

    _stop_nodes()
    try:
        run_dir = _start_nodes(number_of_nodes)

        if not _wait_until_ready(base_port, number_of_nodes):
            raise RuntimeError("timed out waiting for {} nodes to become reachable".format(number_of_nodes))

        events_path = run_dir / "paper_events.jsonl"
        history_path = run_dir / "paper_pull_history.jsonl"
        history_totals_path = run_dir / "paper_pull_totals.jsonl"
        active_duration_sec, _ = _run_active_window(
            spec_for_run,
            base_port,
            number_of_nodes,
            run_index,
            seed,
            events_path,
            history_path=history_path,
            history_totals_path=history_totals_path,
        )
        manifest, summary_row, watch_rows_case, evidence = _collect_evidence(
            spec=spec_for_run,
            run_dir=run_dir,
            events_path=events_path,
            base_port=base_port,
            number_of_nodes=number_of_nodes,
            run_index=run_index,
            seed=seed,
            active_duration_sec=active_duration_sec,
        )
        _write_run_reports(
            run_dir,
            manifest,
            summary_row,
            watch_rows_case,
            evidence,
            events_path,
            history_path=history_path,
            history_totals_path=history_totals_path,
        )
        return {
            "run_dir": run_dir,
            "manifest": manifest,
            "summary_row": summary_row,
            "watch_rows": watch_rows_case,
            "evidence": evidence,
        }
    finally:
        _stop_nodes()


def run_suite(spec, dry_run=False, max_runs=None, node_counts_override=None, duration_sec_override=None):
    _validate_spec(spec)
    spec_for_run = dict(spec)
    if duration_sec_override is not None:
        spec_for_run["duration_sec"] = int(duration_sec_override)
    cases = _suite_case_rows(spec, max_runs=max_runs, node_counts_override=node_counts_override)
    report_dir = _report_dir_for_spec(spec_for_run)

    if dry_run:
        dry_payload = {
            "report_dir": str(report_dir),
            "cases": cases,
            "phase_id": spec_for_run.get("phase_id"),
            "challenge": spec_for_run.get("challenge"),
            "duration_sec": spec_for_run.get("duration_sec"),
        }
        _write_json(report_dir / "dry_run_manifest.json", dry_payload)
        print(json.dumps(dry_payload, indent=2))
        return report_dir

    summary_rows = []
    watch_rows = []

    for case in cases:
        result = _run_case(spec_for_run, case)
        summary_rows.append(result["summary_row"])
        watch_rows.extend(result["watch_rows"])
        _write_suite_reports(report_dir, spec_for_run, summary_rows, watch_rows)

    _write_suite_reports(report_dir, spec_for_run, summary_rows, watch_rows)
    return report_dir


def main():
    parser = argparse.ArgumentParser(description="Run phase-based paper evaluation demos")
    parser.add_argument("--spec", required=True, help="Path to a phase spec JSON file")
    parser.add_argument("--dry-run", action="store_true", help="Validate the spec and emit the planned cases without starting nodes")
    parser.add_argument("--max-runs", type=int, help="Optional cap for run_count while testing the suite")
    parser.add_argument("--node-counts", help="Optional comma-separated override for node counts, e.g. 49,64")
    parser.add_argument("--duration-sec", type=int, help="Optional override for a shorter same-machine smoke test duration")
    args = parser.parse_args()

    spec_path = Path(args.spec).resolve()
    spec = _load_spec(spec_path)

    node_counts_override = None
    if args.node_counts:
        node_counts_override = [int(item.strip()) for item in str(args.node_counts).split(",") if len(item.strip()) > 0]

    try:
        report_dir = run_suite(
            spec=spec,
            dry_run=bool(args.dry_run),
            max_runs=args.max_runs,
            node_counts_override=node_counts_override,
            duration_sec_override=args.duration_sec,
        )
    except Exception as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        sys.exit(1)

    print("Report directory: {}".format(str(report_dir)))


if __name__ == "__main__":
    main()
