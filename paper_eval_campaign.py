#!/usr/bin/env python3
"""Run one batch of every scenario spec as a single paper-eval campaign."""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import paper_eval_runner as runner


ROOT_DIR = Path(__file__).resolve().parent
CAMPAIGN_REPORTS_DIR = ROOT_DIR / "campaign_reports"

CAMPAIGN_FIELDS = [
    "batch_index",
    "scenario_label",
    "phase_id",
    "challenge",
    "nodes",
    "seed",
    "duration_sec",
    "total_mb",
    "tx_fail_total",
    "tx_timeout_total",
    "status",
]

def _load_campaign_spec(path):
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    payload["_spec_path"] = str(path)
    return payload


def _resolve_scenario_specs(campaign_spec):
    spec_dir = Path(campaign_spec["_spec_path"]).resolve().parent
    scenario_specs = []
    for ref in campaign_spec.get("scenario_specs", []):
        path = Path(ref)
        if not path.is_absolute():
            path = (spec_dir / path).resolve()
        spec = runner._load_spec(path)
        runner._validate_spec(spec)
        scenario_specs.append(spec)
    if not scenario_specs:
        raise ValueError("campaign spec must include at least one scenario spec")
    return scenario_specs


def _campaign_dir(campaign_id, stamp):
    CAMPAIGN_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = CAMPAIGN_REPORTS_DIR / "{}_{}".format(campaign_id, stamp)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _scenario_report_dir(campaign_id, suite_id, stamp):
    runner.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = runner.REPORTS_DIR / "{}_{}_{}".format(campaign_id, suite_id, stamp)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _scenario_label(spec):
    return runner._scenario_label(spec.get("phase_id", ""), spec.get("challenge", ""))


def _render_campaign_html(campaign_dir, campaign_spec, scenario_entries, batch_rows, planned_rows=None, current_row=None, state="RUNNING", started_at=None):
    protocol = str(campaign_spec.get("protocol", "")).upper()
    run_count = int(campaign_spec.get("run_count", 1))
    node_counts = campaign_spec.get("node_counts", [])
    rows_for_display = list(batch_rows or planned_rows or [])
    total_planned = len(planned_rows or rows_for_display)
    completed = sum(1 for row in rows_for_display if str(row.get("status", "")).strip().upper() in ("OK", "WARN", "FAILED", "ERROR"))
    running = sum(1 for row in rows_for_display if str(row.get("status", "")).strip().upper() == "RUNNING")
    pct = 100.0 * float(completed) / float(max(1, total_planned))
    started = float(started_at or time.time())
    elapsed_min = max(0.0, (time.time() - started) / 60.0)
    cards = [
        {
            "label": "Protocol",
            "value": protocol,
            "note": str(campaign_spec.get("campaign_name", campaign_spec.get("campaign_id", "Campaign"))),
            "tone": "accent",
        },
        {
            "label": "Batches",
            "value": str(run_count),
            "note": "1 batch = 1 run of each scenario",
            "tone": "accent",
        },
        {
            "label": "Scenarios",
            "value": str(len(scenario_entries)),
            "note": ", ".join(entry["label"] for entry in scenario_entries),
            "tone": "accent",
        },
        {
            "label": "Node Counts",
            "value": ", ".join(str(value) for value in node_counts),
            "note": "{} total scenario runs planned".format(len(scenario_entries) * len(node_counts) * run_count),
            "tone": "accent",
        },
        {
            "label": "Live Status",
            "value": str(state).upper(),
            "note": "{} / {} completed, {} running".format(completed, total_planned, running),
            "tone": "accent",
        },
    ]

    scenario_links = []
    for entry in scenario_entries:
        href = entry["report_dir"].relative_to(campaign_dir.parent.parent)
        scenario_links.append((str(Path("..") / ".." / href / "index.html"), "{} Dashboard".format(entry["label"])))

    current = current_row or {}
    live_rows = [
        {"field": "State", "value": str(state).upper()},
        {"field": "Completed", "value": "{}/{}".format(completed, total_planned)},
        {"field": "Progress", "value": "{}%".format(round(pct, 1))},
        {"field": "Elapsed", "value": "{} min".format(round(elapsed_min, 1))},
        {"field": "Current Scenario", "value": current.get("scenario_label", "")},
        {"field": "Current Batch", "value": current.get("batch_index", "")},
        {"field": "Current Nodes", "value": current.get("nodes", "")},
        {"field": "Current Seed", "value": current.get("seed", "")},
    ]
    status_note = "This dashboard auto-refreshes while the campaign is running. Keep it open beside your terminal tails."
    if str(state).upper() in ("DONE", "FAILED"):
        status_note = "Campaign finished. Use the scenario dashboards below for final averages, node-count comparisons, and figures."
    auto_refresh = ""
    if str(state).upper() not in ("DONE", "FAILED", "DRY RUN"):
        auto_refresh = "<script>setTimeout(function(){ window.location.reload(); }, 5000);</script>"

    sections = [
        """
<section>
  <h2>Live Campaign Progress</h2>
  <p class="section-note">{note}</p>
  <div class="progress-shell"><span style="width:{pct:.1f}%"></span></div>
</section>
""".format(note=status_note, pct=pct),
        runner._render_table_html(
            "Current Batch",
            live_rows,
            ["field", "value"],
            "This is the browser version of the live run status, while Terminal 2 tails events and Terminal 3 tails node logs.",
        ),
        runner._render_links_html(
            "Scenario Dashboards",
            scenario_links,
        ),
        runner._render_table_html(
            "Batch Overview",
            rows_for_display,
            CAMPAIGN_FIELDS,
            "Each batch runs every scenario once. This table lets you confirm that batch 1, 2, 3, and so on stayed aligned across scenarios.",
        ),
        "<details><summary>Show Full Batch Table</summary>{}</details>".format(
            runner._render_table_html("All Batch Fields", batch_rows, CAMPAIGN_FIELDS)
        ),
    ]
    subtitle = "{} | {} second window".format(
        campaign_spec.get("campaign_name", campaign_spec.get("campaign_id", "Campaign")),
        campaign_spec.get("duration_sec", ""),
    )
    runner._write_text(
        campaign_dir / "index.html",
        runner._html_page("Paper Eval Campaign", subtitle, runner._render_cards_html(cards), "".join(sections), script_html=auto_refresh),
    )


def run_campaign(campaign_spec, dry_run=False, max_batches=None, node_counts_override=None, duration_sec_override=None, base_port_override=None, open_live=False, batch_start=1):
    scenario_specs = _resolve_scenario_specs(campaign_spec)
    protocol = str(scenario_specs[0].get("protocol", "")).strip().lower()
    for spec in scenario_specs[1:]:
        if str(spec.get("protocol", "")).strip().lower() != protocol:
            raise ValueError("all scenario specs in a campaign must use the same protocol")

    campaign_id = str(campaign_spec.get("campaign_id", "")).strip()
    if not campaign_id:
        raise ValueError("campaign_id is required")

    total_batches = runner._to_int(campaign_spec.get("run_count", 1), 1)
    batch_start = max(1, runner._to_int(batch_start, 1))
    if batch_start > int(total_batches):
        raise ValueError("batch_start {} is beyond configured run_count {}".format(int(batch_start), int(total_batches)))
    run_count = int(total_batches) - int(batch_start) + 1
    if max_batches is not None:
        run_count = min(int(run_count), int(max_batches))
    batch_end = int(batch_start) + int(run_count) - 1
    batch_indices = list(range(int(batch_start), int(batch_end) + 1))
    view_spec = dict(campaign_spec)
    view_spec["run_count"] = int(run_count)
    view_spec["batch_start"] = int(batch_start)
    view_spec["batch_end"] = int(batch_end)
    node_counts = list(node_counts_override or campaign_spec.get("node_counts", []))
    if not node_counts:
        raise ValueError("campaign node_counts must be a non-empty list")
    duration_sec = runner._to_int(duration_sec_override or campaign_spec.get("duration_sec", 60), 60)
    seed_base = runner._to_int(campaign_spec.get("seed_base", 1000), 1000)

    base_port_for_stamp = runner._to_int(base_port_override or campaign_spec.get("base_port", 0), 0)
    stamp_suffix = "_p{}".format(base_port_for_stamp) if base_port_for_stamp > 0 else ""
    base_stamp = "{}{}".format(time.strftime("%Y%m%d_%H%M%S"), stamp_suffix)
    stamp = base_stamp
    collision_index = 2
    while (CAMPAIGN_REPORTS_DIR / "{}_{}".format(campaign_id, stamp)).exists():
        stamp = "{}_r{}".format(base_stamp, collision_index)
        collision_index += 1
    campaign_dir = _campaign_dir(campaign_id, stamp)
    scenario_entries = []
    for spec in scenario_specs:
        scenario_spec = dict(spec)
        scenario_spec["duration_sec"] = int(duration_sec)
        if base_port_override is not None:
            scenario_spec["base_port"] = int(base_port_override)
        scenario_spec["suite_id"] = "{}_{}".format(campaign_id, spec.get("suite_id", "suite"))
        report_dir = _scenario_report_dir(campaign_id, scenario_spec["suite_id"], stamp)
        scenario_entries.append(
            {
                "spec": scenario_spec,
                "label": _scenario_label(scenario_spec),
                "report_dir": report_dir,
                "summary_rows": [],
                "watch_rows": [],
            }
        )

    plan_rows = []
    for batch_index in batch_indices:
        seed = int(seed_base + batch_index - 1)
        for node_count in node_counts:
            for entry in scenario_entries:
                plan_rows.append(
                    {
                        "batch_index": batch_index,
                        "scenario_label": entry["label"],
                        "phase_id": entry["spec"].get("phase_id", ""),
                        "challenge": entry["spec"].get("challenge", ""),
                        "nodes": int(node_count),
                        "seed": seed,
                        "duration_sec": int(duration_sec),
                        "total_mb": "",
                        "tx_fail_total": "",
                        "tx_timeout_total": "",
                        "status": "PLANNED",
                    }
                )

    if dry_run:
        runner._write_json(
            campaign_dir / "dry_run_manifest.json",
            {
                "campaign_id": campaign_id,
                "protocol": protocol,
                "run_count": run_count,
                "batch_start": int(batch_start),
                "batch_end": int(batch_end),
                "node_counts": node_counts,
                "duration_sec": duration_sec,
                "base_port": int(base_port_override) if base_port_override is not None else "",
                "scenario_specs": [entry["spec"].get("_spec_path", "") for entry in scenario_entries],
            },
        )
        runner._write_tsv(campaign_dir / "campaign_runs.tsv", plan_rows, CAMPAIGN_FIELDS)
        _render_campaign_html(campaign_dir, view_spec, scenario_entries, plan_rows, planned_rows=plan_rows, state="DRY RUN")
        return campaign_dir

    started_at = time.time()
    live_rows = [dict(row) for row in plan_rows]
    runner._write_text(campaign_dir / "LATEST_CAMPAIGN_DIR.txt", str(campaign_dir) + "\n")
    runner._write_tsv(campaign_dir / "campaign_runs.tsv", live_rows, CAMPAIGN_FIELDS)
    _render_campaign_html(campaign_dir, view_spec, scenario_entries, live_rows, planned_rows=plan_rows, state="RUNNING", started_at=started_at)
    print("Live campaign dashboard: {}/index.html".format(campaign_dir), flush=True)
    if open_live:
        try:
            subprocess.run(["open", str(campaign_dir / "index.html")], check=False)
        except Exception:
            pass

    row_index = 0
    for batch_index in batch_indices:
        seed = int(seed_base + batch_index - 1)
        for node_count in node_counts:
            for entry in scenario_entries:
                current_row = live_rows[row_index]
                current_row["status"] = "RUNNING"
                current_row["total_mb"] = ""
                current_row["tx_fail_total"] = ""
                current_row["tx_timeout_total"] = ""
                runner._write_tsv(campaign_dir / "campaign_runs.tsv", live_rows, CAMPAIGN_FIELDS)
                _render_campaign_html(
                    campaign_dir,
                    view_spec,
                    scenario_entries,
                    live_rows,
                    planned_rows=plan_rows,
                    current_row=current_row,
                    state="RUNNING",
                    started_at=started_at,
                )
                case = {
                    "nodes": int(node_count),
                    "run_index": int(batch_index),
                    "seed": seed,
                }
                try:
                    result = runner._run_case(entry["spec"], case)
                except Exception:
                    current_row["status"] = "FAILED"
                    runner._write_tsv(campaign_dir / "campaign_runs.tsv", live_rows, CAMPAIGN_FIELDS)
                    _render_campaign_html(
                        campaign_dir,
                        view_spec,
                        scenario_entries,
                        live_rows,
                        planned_rows=plan_rows,
                        current_row=current_row,
                        state="FAILED",
                        started_at=started_at,
                    )
                    raise
                entry["summary_rows"].append(result["summary_row"])
                entry["watch_rows"].extend(result["watch_rows"])
                runner._write_suite_reports(entry["report_dir"], entry["spec"], entry["summary_rows"], entry["watch_rows"], full_figures=False)
                current_row.update(
                    {
                        "total_mb": result["summary_row"].get("total_mb", ""),
                        "tx_fail_total": result["summary_row"].get("tx_fail_total", ""),
                        "tx_timeout_total": result["summary_row"].get("tx_timeout_total", ""),
                        "status": result["summary_row"].get("status", ""),
                    }
                )
                runner._write_tsv(campaign_dir / "campaign_runs.tsv", live_rows, CAMPAIGN_FIELDS)
                _render_campaign_html(
                    campaign_dir,
                    view_spec,
                    scenario_entries,
                    live_rows,
                    planned_rows=plan_rows,
                    current_row=current_row,
                    state="RUNNING",
                    started_at=started_at,
                )
                row_index += 1

    for entry in scenario_entries:
        runner._write_suite_reports(entry["report_dir"], entry["spec"], entry["summary_rows"], entry["watch_rows"], full_figures=True)

    runner._write_json(
        campaign_dir / "campaign_manifest.json",
        {
            "campaign_id": campaign_id,
            "campaign_name": campaign_spec.get("campaign_name", campaign_id),
            "protocol": protocol,
            "run_count": run_count,
            "batch_start": int(batch_start),
            "batch_end": int(batch_end),
            "total_batches": int(total_batches),
            "node_counts": node_counts,
            "duration_sec": duration_sec,
            "base_port": base_port_for_stamp if base_port_for_stamp > 0 else "",
            "scenario_reports": {
                entry["label"]: str(entry["report_dir"])
                for entry in scenario_entries
            },
        },
    )
    _render_campaign_html(campaign_dir, view_spec, scenario_entries, live_rows, planned_rows=plan_rows, state="DONE", started_at=started_at)
    return campaign_dir


def main():
    parser = argparse.ArgumentParser(description="Run one batch of every paper-eval scenario as a single campaign")
    parser.add_argument("--spec", required=True, help="Path to a campaign spec JSON file")
    parser.add_argument("--dry-run", action="store_true", help="Validate and emit the planned batch matrix without starting nodes")
    parser.add_argument("--max-batches", type=int, help="Optional cap for the campaign run_count while testing")
    parser.add_argument("--node-counts", help="Optional comma-separated override for node counts, e.g. 49,64")
    parser.add_argument("--duration-sec", type=int, help="Optional duration override, for example 60 or 120")
    parser.add_argument("--base-port", type=int, help="Optional base port override for shared-server lab runs")
    parser.add_argument("--open-live", action="store_true", help="Open the campaign dashboard immediately and auto-refresh while the campaign runs")
    parser.add_argument("--batch-start", type=int, default=1, help="First batch index to execute when chunking the campaign")
    args = parser.parse_args()

    spec_path = Path(args.spec).resolve()
    campaign_spec = _load_campaign_spec(spec_path)

    node_counts_override = None
    if args.node_counts:
        node_counts_override = [int(item.strip()) for item in str(args.node_counts).split(",") if item.strip()]

    try:
        campaign_dir = run_campaign(
            campaign_spec=campaign_spec,
            dry_run=bool(args.dry_run),
            max_batches=args.max_batches,
            node_counts_override=node_counts_override,
            duration_sec_override=args.duration_sec,
            base_port_override=args.base_port,
            open_live=bool(args.open_live),
            batch_start=args.batch_start,
        )
    except Exception as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        sys.exit(1)

    print("Campaign directory: {}".format(campaign_dir))


if __name__ == "__main__":
    main()
