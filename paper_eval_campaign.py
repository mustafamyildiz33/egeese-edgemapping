#!/usr/bin/env python3
"""Run one batch of every scenario spec as a single paper-eval campaign."""

import argparse
import json
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


def _render_campaign_html(campaign_dir, campaign_spec, scenario_entries, batch_rows):
    protocol = str(campaign_spec.get("protocol", "")).upper()
    run_count = int(campaign_spec.get("run_count", 1))
    node_counts = campaign_spec.get("node_counts", [])
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
    ]

    scenario_links = []
    for entry in scenario_entries:
        href = entry["report_dir"].relative_to(campaign_dir.parent.parent)
        scenario_links.append((str(Path("..") / ".." / href / "index.html"), "{} Dashboard".format(entry["label"])))

    sections = [
        runner._render_links_html(
            "Scenario Dashboards",
            scenario_links,
        ),
        runner._render_table_html(
            "Batch Overview",
            batch_rows,
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
        runner._html_page("Paper Eval Campaign", subtitle, runner._render_cards_html(cards), "".join(sections)),
    )


def run_campaign(campaign_spec, dry_run=False, max_batches=None, node_counts_override=None, duration_sec_override=None):
    scenario_specs = _resolve_scenario_specs(campaign_spec)
    protocol = str(scenario_specs[0].get("protocol", "")).strip().lower()
    for spec in scenario_specs[1:]:
        if str(spec.get("protocol", "")).strip().lower() != protocol:
            raise ValueError("all scenario specs in a campaign must use the same protocol")

    campaign_id = str(campaign_spec.get("campaign_id", "")).strip()
    if not campaign_id:
        raise ValueError("campaign_id is required")

    run_count = runner._to_int(campaign_spec.get("run_count", 1), 1)
    if max_batches is not None:
        run_count = min(int(run_count), int(max_batches))
    node_counts = list(node_counts_override or campaign_spec.get("node_counts", []))
    if not node_counts:
        raise ValueError("campaign node_counts must be a non-empty list")
    duration_sec = runner._to_int(duration_sec_override or campaign_spec.get("duration_sec", 60), 60)
    seed_base = runner._to_int(campaign_spec.get("seed_base", 1000), 1000)

    stamp = time.strftime("%Y%m%d_%H%M%S")
    campaign_dir = _campaign_dir(campaign_id, stamp)
    scenario_entries = []
    for spec in scenario_specs:
        scenario_spec = dict(spec)
        scenario_spec["duration_sec"] = int(duration_sec)
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
    for batch_index in range(1, int(run_count) + 1):
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
                "node_counts": node_counts,
                "duration_sec": duration_sec,
                "scenario_specs": [entry["spec"].get("_spec_path", "") for entry in scenario_entries],
            },
        )
        runner._write_tsv(campaign_dir / "campaign_runs.tsv", plan_rows, CAMPAIGN_FIELDS)
        _render_campaign_html(campaign_dir, campaign_spec, scenario_entries, plan_rows)
        return campaign_dir

    batch_rows = []
    for batch_index in range(1, int(run_count) + 1):
        seed = int(seed_base + batch_index - 1)
        for node_count in node_counts:
            for entry in scenario_entries:
                case = {
                    "nodes": int(node_count),
                    "run_index": int(batch_index),
                    "seed": seed,
                }
                result = runner._run_case(entry["spec"], case)
                entry["summary_rows"].append(result["summary_row"])
                entry["watch_rows"].extend(result["watch_rows"])
                runner._write_suite_reports(entry["report_dir"], entry["spec"], entry["summary_rows"], entry["watch_rows"])
                batch_rows.append(
                    {
                        "batch_index": int(batch_index),
                        "scenario_label": entry["label"],
                        "phase_id": entry["spec"].get("phase_id", ""),
                        "challenge": entry["spec"].get("challenge", ""),
                        "nodes": int(node_count),
                        "seed": seed,
                        "duration_sec": int(duration_sec),
                        "total_mb": result["summary_row"].get("total_mb", ""),
                        "tx_fail_total": result["summary_row"].get("tx_fail_total", ""),
                        "tx_timeout_total": result["summary_row"].get("tx_timeout_total", ""),
                        "status": result["summary_row"].get("status", ""),
                    }
                )
                runner._write_tsv(campaign_dir / "campaign_runs.tsv", batch_rows, CAMPAIGN_FIELDS)
                _render_campaign_html(campaign_dir, campaign_spec, scenario_entries, batch_rows)

    runner._write_json(
        campaign_dir / "campaign_manifest.json",
        {
            "campaign_id": campaign_id,
            "campaign_name": campaign_spec.get("campaign_name", campaign_id),
            "protocol": protocol,
            "run_count": run_count,
            "node_counts": node_counts,
            "duration_sec": duration_sec,
            "scenario_reports": {
                entry["label"]: str(entry["report_dir"])
                for entry in scenario_entries
            },
        },
    )
    _render_campaign_html(campaign_dir, campaign_spec, scenario_entries, batch_rows)
    return campaign_dir


def main():
    parser = argparse.ArgumentParser(description="Run one batch of every paper-eval scenario as a single campaign")
    parser.add_argument("--spec", required=True, help="Path to a campaign spec JSON file")
    parser.add_argument("--dry-run", action="store_true", help="Validate and emit the planned batch matrix without starting nodes")
    parser.add_argument("--max-batches", type=int, help="Optional cap for the campaign run_count while testing")
    parser.add_argument("--node-counts", help="Optional comma-separated override for node counts, e.g. 49,64")
    parser.add_argument("--duration-sec", type=int, help="Optional duration override, for example 60 or 120")
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
        )
    except Exception as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        sys.exit(1)

    print("Campaign directory: {}".format(campaign_dir))


if __name__ == "__main__":
    main()
