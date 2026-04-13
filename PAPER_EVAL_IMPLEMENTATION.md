# Paper Evaluation Implementation Guide

This document explains the current paper-evaluation harness for EGESS, how it is organized, what it measures today, and how the team should use it for repeatable experiments and upcoming protocol comparisons.

## Purpose

The goal of this implementation is to support a strong evaluation section for the paper by making experiments:

- repeatable,
- phase-based,
- easy to run for exact `60s` or `120s`,
- easy to summarize for Excel, figures, and GitHub,
- ready to extend to a second protocol later.

This implementation currently supports `EGESS` directly. The comparison protocol adapter is not implemented yet, but the evaluation structure is designed so that the second protocol can be added under the same experiment contract.

The React demo now also includes an in-app `Paper Evaluation Runbook` panel so the phase structure, commands, outputs, and push checklist are visible in the frontend demo and not only in terminal-oriented documentation.

All evaluation and testing runs are designed to happen on one host at a time. For a shared lab server, simultaneous users must choose different base ports such as `9000`, `9100`, `9200`, and stop only their own base port.

## What Was Added

### 1. Phase-based evaluation runner

New file:

- `paper_eval_runner.py`

This runner:

- reads a JSON phase spec,
- starts a fresh node deployment,
- waits for the network to become reachable,
- executes the active scenario window for an exact `duration_sec`,
- collects node-level evidence,
- writes per-run reports,
- writes suite-level combined reports.

The exact scenario window is enforced by a monotonic timer in the runner. Startup time is not counted as part of the active `60s` or `120s` evaluation window.

### 2. Three paper phases

The harness is organized into three experimental phases:

#### Phase 1: Fair Baseline

Challenge:

- `steady_state_baseline`

Purpose:

- clean traffic comparison,
- steady-state message load,
- byte overhead,
- throughput,
- per-node load,
- scalability at `49`, `64`, and `81` nodes.

Specs:

- `paper_eval/phase1/phase1_baseline_60s.json`
- `paper_eval/phase1/phase1_baseline_120s.json`

#### Phase 2: Hazard Sensing

Challenge:

- `tornado_sweep`

Purpose:

- structured moving-hazard experiments,
- local hazard behavior,
- far sensing behavior,
- topology growth analysis across `49`, `64`, and `81` nodes.

Specs:

- `paper_eval/phase2/phase2_hazard_60s.json`
- `paper_eval/phase2/phase2_hazard_120s.json`

#### Phase 3: Adversarial Stress

Challenge:

- `ghost_outage_noise`

Purpose:

- false unavailability,
- misleading sensor behavior,
- noisy sensing,
- protocol robustness under adversarial conditions.

Specs:

- `paper_eval/phase3/phase3_stress_60s.json`
- `paper_eval/phase3/phase3_stress_120s.json`

## Current Scenario Behavior

### Phase 1

Baseline traffic only:

- no injected crashes,
- no synthetic sensor noise,
- periodic trigger traffic only.

### Phase 2

Tornado sweep:

- a seeded destruction path is generated across the grid,
- nodes are crashed in batches to simulate a moving tornado,
- later in the run those nodes are marked as recovering,
- finally they are reset to normal.

The same seed can be reused across network sizes so comparisons are paired and fair.

### Phase 3

Stress sequence:

- one node is temporarily forced into `crash_sim`,
- one neighbor is forced into `lie_sensor`,
- one neighbor is forced into `flap`,
- a short recovery/reset sequence is applied near the end.

This phase is intended to stress resilience and false-positive behavior.

## Runtime Changes Added

To make the evaluation meaningful, two runtime improvements were added.

### 1. Active fault behavior for `lie_sensor` and `flap`

Previously these flags existed, but they did not visibly perturb runtime sensing behavior.

Now:

- `lie_sensor` forces a node to present `ALERT`,
- `flap` alternates the node between `ALERT` and `NORMAL` using the configured fault period.

This behavior is implemented in:

- `background_protocol.py`

### 2. Message byte accounting

The system already tracked message counts. It now also tracks:

- receive bytes,
- transmit bytes,
- total bytes.

This gives us honest byte totals and MB ballparks for the paper.

Updated files:

- `egess_api.py`
- `listener_protocol.py`
- `node_state_init.json`

## How To Run

## Dry run only

This validates a phase spec and prints the planned cases without starting nodes:

```bash
python3 paper_eval_runner.py --spec paper_eval/phase1/phase1_baseline_60s.json --dry-run
```

## Lightweight test run

This is useful before launching a full 30-run suite:

```bash
python3 paper_eval_runner.py --spec paper_eval/phase2/phase2_hazard_60s.json --max-runs 1 --node-counts 49
```

## Same-machine smoke test

This is the fastest end-to-end validation mode for one local computer:

```bash
python3 paper_eval_runner.py --spec paper_eval/phase2/phase2_hazard_60s.json --max-runs 1 --node-counts 49 --duration-sec 10
```

## Full one-minute suite

```bash
python3 paper_eval_runner.py --spec paper_eval/phase3/phase3_stress_60s.json
```

## Full two-minute suite

```bash
python3 paper_eval_runner.py --spec paper_eval/phase2/phase2_hazard_120s.json
```

## Input Structure

Each spec file currently defines:

- `suite_id`
- `phase_id`
- `phase_name`
- `protocol`
- `challenge`
- `duration_sec`
- `base_port`
- `trigger_interval_sec`
- `node_counts`
- `run_count`
- `seed_base`
- `scenario`

The current EGESS specs use:

- node counts: `49`, `64`, `81`
- run count: `30`
- duration: `60` or `120`

## Output Structure

### Per-run outputs

Each run writes into its normal run directory:

- `paper_events.jsonl`
- `paper_manifest.json`
- `paper_evidence.json`
- `paper_summary.tsv`
- `paper_watch_nodes.tsv`
- `paper_summary.md`

### Suite-level outputs

Each suite writes into:

- `paper_reports/<suite_id>_<timestamp>/`

This folder includes:

- `all_runs.tsv`
- `all_watch_nodes.tsv`
- `summary_by_nodes.tsv`
- `README.md`

## Why The Reports Are Useful

The reports are designed for:

- Excel import,
- quick figure building,
- team review,
- paper appendix material,
- GitHub documentation.

`TSV` was chosen because it pastes directly into Excel cleanly without manual CSV cleanup.

## What The Reports Capture Today

The current implementation already captures:

- exact requested duration and measured active duration,
- node count,
- run index,
- seed,
- watched local node,
- watched far node,
- reachable node count,
- message totals,
- byte totals,
- MB totals,
- transmission failures and timeouts,
- watched-node protocol state,
- watched-node boundary kind,
- watched-node scores,
- watched-node tomography snapshot fields such as direction, phase, distance, and ETA,
- watched-node fault flags at collection time.

## Recommended Team Workflow

### 1. Agree on the exact paper contract

For both EGESS and the comparison protocol:

- same topology,
- same node counts,
- same seeds,
- same run counts,
- same active duration,
- same trigger interval,
- same watched-node selection rule.

### 2. Run EGESS first

Start with:

- `phase1_baseline_60s`
- `phase2_hazard_60s`
- `phase3_stress_60s`

Then repeat with the `120s` variants if the team decides to include the longer experiment set.

### 3. Add the comparison protocol under the same structure

The cleanest next step is to give the check-in system:

- its own runner adapter,
- the same phase spec format,
- the same output column names.

That way the combined comparison tables can be merged directly.

## Important Current Limitations

This implementation is strong enough to structure the experiments, but it is not the final paper analysis layer yet.

### Implemented now

- phase structure,
- exact `60s` / `120s` active windows,
- seeded runs,
- watched-node reporting,
- byte accounting,
- real stress-fault behavior for `lie_sensor` and `flap`,
- suite-level TSV output.

### Not fully automated yet

- direct EGESS-vs-check-in comparison runs,
- external accuracy scoring against ground-truth labels,
- first-event extraction such as first `WATCH`, first `IMPACT`, first `RECOVERING`,
- aggregated figure generation,
- latency percentile reporting from explicit end-to-end message timestamps,
- polished Excel color formatting generated automatically.

Those should be treated as the next layer of paper tooling, not as missing core harness functionality.

## Suggested Next Step

The next implementation step should be one of these:

1. Add the check-in protocol adapter using the same spec and output schema.
2. Add a post-processing script that computes first-event metrics and summary figures from `paper_events.jsonl` and `paper_watch_nodes.tsv`.
3. Add an Excel-export helper that turns `TSV` results into a color-coded workbook.

## Files To Share With The Team

If you want to show the team the implementation entry points, send these files first:

- `PAPER_EVAL_IMPLEMENTATION.md`
- `paper_eval/README.md`
- `paper_eval_runner.py`
- `paper_eval/phase1/phase1_baseline_60s.json`
- `paper_eval/phase2/phase2_hazard_60s.json`
- `paper_eval/phase3/phase3_stress_60s.json`

These are enough for someone to understand the design, run a dry-run, and inspect the evaluation structure.
