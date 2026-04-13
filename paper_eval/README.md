# Paper Evaluation Specs

These JSON files define the phase-based paper demo suites that the paper runner
can execute with an exact active demo window of either 60 or 120 seconds.

## Phase layout

- `phase1`: Baseline. No destructive event is injected. This phase measures clean steady-state reachability, throughput, overhead, and per-node load.
- `phase2`: Fire Spread And Bomb. A center ignition spreads outward in hop-based rings, marks a temporary bomb core, then trails recovery behind the front. This is the main spread/accuracy phase.
- `phase3`: Tornado Hazard Sensing. A moving tornado band crosses the grid. The local watch node shows direct hazard detection while the far watch node shows propagation across distance.
- `phase4`: Adversarial Stress. The runner injects false unavailability, noisy or lying sensors, and unstable/flapping behavior. This tests resilience, false positives, false unavailable references, and recovery.

## Durations

Each phase includes:

- a `60s` spec for the exact one-minute runs your teammate requested,
- a `120s` spec for the two-minute comparison run you may want later.

## Runner examples

Dry-run a suite without starting nodes:

```bash
python3 paper_eval_runner.py --spec paper_eval/phase1/phase1_baseline_60s.json --dry-run
```

Run one suite with only the first two repetitions while testing:

```bash
python3 paper_eval_runner.py --spec paper_eval/phase2/phase2_fire_60s.json --max-runs 2
```

Override node counts while testing:

```bash
python3 paper_eval_runner.py --spec paper_eval/phase4/phase4_stress_120s.json --max-runs 1 --node-counts 49
```

Short same-machine smoke test:

```bash
python3 paper_eval_runner.py --spec paper_eval/phase2/phase2_fire_60s.json --max-runs 1 --node-counts 49 --duration-sec 10
```

## Copy-Paste Commands

For the school-lab run sheet, use:

- `paper_eval/LAB_RUN_SHEET.md`
- `paper_eval/run_sheets/egess_proof_lite_batch_sheet.csv`
- `paper_eval/run_sheets/checkin_proof_lite_batch_sheet.csv`

## Storage Safety

The runner is storage-safe by default for class demos and paper collection:

- Node logs are bounded to `16 KB` per node by default.
- The wrapper checks available disk space before starting and estimates reports, history, evidence, and capped logs.
- `--dry-run` prints the same storage estimate without starting nodes.
- Protocol verbose logging is off by default, so `data.csv` and raw node logs do not balloon.
- Evidence JSON is compact by default and written without extra pretty-print whitespace.
- The final HTML dashboard does not embed the hex replay by default.
- Sampled history defaults to the local/far watched nodes instead of every node.
- Raw node log tails are not embedded into HTML unless explicitly enabled.
- Reports, TSV files, and HTML dashboards are still generated normally in light mode.

To re-enable heavier debugging views for a short local run only:

```bash
cd /Users/mustafa/egess
EGESS_HTML_REPLAY=1 EGESS_HISTORY_SCOPE=all EGESS_HTML_NODE_LOG_LINES=20 ./run_paper_eval.sh --mode phase2 --duration 60 --batches 1 --nodes 16
```

For the real 30-batch paper/GitHub collection, use lean-graphs mode. It keeps
the final suite dashboards and comparison figure exports, while skipping node
logs, per-run HTML, live HTML, and per-run figures:

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 30 --nodes 49,64 --lean-graphs
```

If you want a small proof trail without risking full raw logs, use proof-lite
instead. It keeps the same final dashboards, paper figures, Google Sheets CSVs,
and export bundle, plus capped `32 KB` node logs and compact evidence snippets:

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 30 --nodes 49,64 --proof-lite
```

For safer lab collection, run the same 30 batches as five 6-batch chunks. The
`--batch-start` value keeps batch numbers and seeds unique across chunks:

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 6 --batch-start 1  --nodes 49,64 --lean-graphs
./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 6 --batch-start 7  --nodes 49,64 --lean-graphs
./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 6 --batch-start 13 --nodes 49,64 --lean-graphs
./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 6 --batch-start 19 --nodes 49,64 --lean-graphs
./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 6 --batch-start 25 --nodes 49,64 --lean-graphs
```

After each 6-batch chunk, run the checkpoint before starting the next one. It
checks the row count, phases, node sizes, seed range, finished statuses, scenario
dashboards, figure data, and Google Sheets CSVs:

```bash
cd /Users/mustafa/egess
./.venv/bin/python check_chunk_status.py --base-port 9100 --batch-start 1 --batches 6 --nodes 49,64
```

For the next chunks, change `--batch-start` to `7`, `13`, `19`, and `25`.

Before starting the real collection, you can verify both protocols plan the
same 4-phase, 49/64-node matrix:

```bash
cd /Users/mustafa/egess
./verify_eval_matrix.sh
```

After those five chunks finish, merge them into complete 30-batch dashboards.
This rebuilds the normal suite HTML, final graphs, figure exports, TSVs, and
grouped comparisons from every chunk:

```bash
cd /Users/mustafa/egess
./.venv/bin/python merge_paper_reports.py --base-port 9100 --nodes 49,64 --duration-sec 60 --expected-batches 30
```

The merged folder is written under `merged_paper_reports/`. Its top-level
`index.html` links to each complete scenario dashboard. If you accidentally
rerun one chunk, the merger keeps the newest copy for that batch/node pair.
The merged dashboards include a `Paper Highlights` section with fastest
detection, cleanest recovery, lowest overhead, reachability, and watched-node
standouts for paper screenshots and captions.
The merged `index.html` also includes a `Download Export Bundle` button. That
ZIP contains the full merged dashboards, combined all-scenario CSVs, Google
Sheets CSVs, figure PNGs, and a laptop import README.

On your personal laptop, unzip the downloaded file and open:

```text
<unzipped-folder>/index.html
```

For Google Sheets, import:

```text
portable_export/combined_all_runs.csv
portable_export/combined_watch_nodes.csv
portable_export/combined_summary_by_nodes.csv
portable_export/combined_metric_averages.csv
```

For the Check-In protocol, use the separate Check-In runner:

```bash
cd /Users/mustafa/egess/external/checkin-egess-eval
./run_paper_eval.sh --base-port 9200 --mode all --duration 60 --batches 6 --batch-start 1  --nodes 49,64 --lean-graphs
./run_paper_eval.sh --base-port 9200 --mode all --duration 60 --batches 6 --batch-start 7  --nodes 49,64 --lean-graphs
./run_paper_eval.sh --base-port 9200 --mode all --duration 60 --batches 6 --batch-start 13 --nodes 49,64 --lean-graphs
./run_paper_eval.sh --base-port 9200 --mode all --duration 60 --batches 6 --batch-start 19 --nodes 49,64 --lean-graphs
./run_paper_eval.sh --base-port 9200 --mode all --duration 60 --batches 6 --batch-start 25 --nodes 49,64 --lean-graphs
```

For Check-In proof-lite collection, use the same commands with `--proof-lite`
instead of `--lean-graphs`.

After each Check-In chunk, run the same checkpoint from the EGESS repo:

```bash
cd /Users/mustafa/egess
./.venv/bin/python check_chunk_status.py --root /Users/mustafa/egess/external/checkin-egess-eval --base-port 9200 --batch-start 1 --batches 6 --nodes 49,64
```

Use data-only only when you want the smallest possible TSV/JSON output and do
not need generated graphs from that run:

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 30 --nodes 49,64 --data-only
```

If you need extra raw node log detail for a short demo, keep it bounded:

```bash
cd /Users/mustafa/egess
EGESS_LOG=1 ./run_paper_eval.sh --mode all --duration 60 --batches 1 --nodes 49 --log-max-kb 128
```

Avoid `--full-logs` unless you intentionally want uncapped raw logs. The wrapper blocks it by default because it can fill a laptop disk.
Avoid `--full-evidence` for paper batches. It is only for short debugging runs because it stores raw node state. Use `--proof-lite` when you want small logs and evidence for paper proof.

Preview storage before a full 30-batch run:

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --mode all --duration 60 --batches 30 --nodes 49,64 --dry-run
./run_paper_eval.sh --mode all --duration 60 --batches 30 --nodes 49,64 --proof-lite --dry-run
```

To clean old raw run folders before the real experiment:

```bash
cd /Users/mustafa/egess
./clean_eval_outputs.sh runs
```

To clear both raw runs and generated report dashboards:

```bash
cd /Users/mustafa/egess
./clean_eval_outputs.sh all
```

## Shared Server Lab Runs

First, check whether the school computers are really doing the compute locally.
On each computer, run:

```bash
hostname
pwd
```

If the hostnames are different, the work is spread across the school computers.
That is the best setup: each machine only runs its own 49/64-node case, not all
four machines' nodes on one CPU.

If every terminal shows the same hostname, everyone is probably logged into one
shared server. In that case, do not start all four full runs at the same second.
Either run only one or two computers at a time, or stagger the starts:

```bash
# Computer 1
./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 6 --batch-start 1 --nodes 49,64 --lean-graphs

# Computer 2
./run_paper_eval.sh --base-port 9200 --mode all --duration 60 --batches 6 --batch-start 1 --nodes 49,64 --lean-graphs --start-delay-min 10

# Computer 3
./run_paper_eval.sh --base-port 9300 --mode all --duration 60 --batches 6 --batch-start 1 --nodes 49,64 --lean-graphs --start-delay-min 20

# Computer 4
./run_paper_eval.sh --base-port 9400 --mode all --duration 60 --batches 6 --batch-start 1 --nodes 49,64 --lean-graphs --start-delay-min 30
```

If multiple lab computers are all connected to the same server, give each
person a different base port. Do not let everybody use `9000` at the same time.

Example assignments:

- Computer 1: `9000`
- Computer 2: `9100`
- Computer 3: `9200`
- Computer 4: `9300`
- Computer 5: `9400`
- Computer 6: `9500`

Quick preflight on a person's assigned base port:

```bash
cd /Users/mustafa/egess
./demo_proof.sh --base-port 9100 --nodes 6 --duration 12 --trigger-interval 2
```

Find that person's latest run folder:

```bash
cd /Users/mustafa/egess
RUN_DIR=$(ls -1dt runs/*_p9100 | head -n 1)
```

Paper run on a person's assigned base port:

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 4 --nodes 49,64
```

Full paper data collection on a person's assigned base port:

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 30 --nodes 49,64 --lean-graphs
```

After everyone finishes, generate one final comparison dashboard across all
base ports present in `paper_reports/`:

```bash
cd /Users/mustafa/egess
./.venv/bin/python lab_compare.py
```

To rebuild complete 30-batch dashboards for one person's chunked run first:

```bash
cd /Users/mustafa/egess
./.venv/bin/python merge_paper_reports.py --base-port 9100 --nodes 49,64 --duration-sec 60 --expected-batches 30
```

If the reports came from different computers, copy each computer's
`paper_reports/` folder onto one machine and label them:

```bash
cd /Users/mustafa/egess
./.venv/bin/python lab_compare.py \
  --root lab1=/path/to/lab1/paper_reports \
  --root lab2=/path/to/lab2/paper_reports \
  --root lab3=/path/to/lab3/paper_reports \
  --root lab4=/path/to/lab4/paper_reports
```

That writes `lab_comparison_reports/<timestamp>/index.html` with final
cross-computer graphs and `lab_overview.tsv`.

Stop only that person's nodes:

```bash
cd /Users/mustafa/egess
./stopnodes --base-port 9100
```

## Demo Order

Use this order during class:

1. Start a quick `4-batch` run with `--open-live`.
2. In a second terminal, tail the live event stream.
3. In a third terminal, tail one or two node logs.
4. When it finishes, open the campaign report.
5. Open the latest scenario suite report.
6. Open the latest single-run deep dive.

### Live Tail Commands

Tail the newest run's event stream:

```bash
cd /Users/mustafa/egess
RUN_DIR=$(ls -1dt runs/* | head -n 1)
tail -f "$RUN_DIR/paper_events.jsonl"
```

Tail the newest run's local and far watch node logs:

```bash
cd /Users/mustafa/egess
RUN_DIR=$(ls -1dt runs/* | head -n 1)
tail -f "$RUN_DIR/node_9024.log" "$RUN_DIR/node_9000.log"
```

Note: node logs are intentionally capped. The event stream is the best live view for the class demo.

Tail the newest run's paper summary files after the run:

```bash
cd /Users/mustafa/egess
RUN_DIR=$(ls -1dt runs/* | head -n 1)
sed -n '1,40p' "$RUN_DIR/paper_summary.tsv"
sed -n '1,40p' "$RUN_DIR/paper_watch_nodes.tsv"
```

### Quick Test: 4 batches, 60 seconds, 49 nodes

Run the quick all-scenarios test:

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --mode all --duration 60 --batches 4 --nodes 49 --open-live
```

The live campaign dashboard opens immediately and auto-refreshes while the batch is running. For the current single-run browser view, open:

```bash
cd /Users/mustafa/egess
RUN_DIR=$(ls -1dt runs/* | head -n 1)
open "$RUN_DIR/live_run.html"
```

Open the campaign report:

```bash
cd /Users/mustafa/egess
CAMPAIGN_DIR=$(ls -1dt campaign_reports/all_together_60s_* | head -n 1)
open "$CAMPAIGN_DIR/index.html"
```

Open the latest scenario suite report:

```bash
cd /Users/mustafa/egess
REPORT_DIR=$(ls -1dt paper_reports/* | head -n 1)
open "$REPORT_DIR/index.html"
```

Open the latest single-run deep dive:

```bash
cd /Users/mustafa/egess
RUN_DIR=$(ls -1dt runs/* | head -n 1)
open "$RUN_DIR/paper_summary.html"
```

### Quick Test: 4 batches, 60 seconds, 40 nodes

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --mode all --duration 60 --batches 4 --nodes 40
```

### Quick Test: 4 batches, 60 seconds, 64 nodes

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --mode all --duration 60 --batches 4 --nodes 64
```

### Quick Test: 4 batches, 60 seconds, 89 nodes

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --mode all --duration 60 --batches 4 --nodes 89
```

### Full Run: 30 batches, 60 seconds, 49 nodes

Run the full all-scenarios paper batch:

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --mode all --duration 60 --batches 30 --nodes 49 --open-live
```

Open the campaign report:

```bash
cd /Users/mustafa/egess
CAMPAIGN_DIR=$(ls -1dt campaign_reports/all_together_60s_* | head -n 1)
open "$CAMPAIGN_DIR/index.html"
```

Open the latest scenario suite report:

```bash
cd /Users/mustafa/egess
REPORT_DIR=$(ls -1dt paper_reports/* | head -n 1)
open "$REPORT_DIR/index.html"
```

Open the latest single-run deep dive:

```bash
cd /Users/mustafa/egess
RUN_DIR=$(ls -1dt runs/* | head -n 1)
open "$RUN_DIR/paper_summary.html"
```

### Full Run: 30 batches, 60 seconds, 40 nodes

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --mode all --duration 60 --batches 30 --nodes 40
```

### Full Run: 30 batches, 60 seconds, 64 nodes

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --mode all --duration 60 --batches 30 --nodes 64
```

### Full Run: 30 batches, 60 seconds, 89 nodes

```bash
cd /Users/mustafa/egess
./run_paper_eval.sh --mode all --duration 60 --batches 30 --nodes 89
```

## Output

Each real run writes:

- `runs/<timestamp>/paper_events.jsonl`
- `runs/<timestamp>/paper_manifest.json`
- `runs/<timestamp>/paper_evidence.json`
- `runs/<timestamp>/paper_summary.tsv`
- `runs/<timestamp>/paper_watch_nodes.tsv`
- `runs/<timestamp>/paper_summary.md`

Each suite also writes a combined report bundle into `paper_reports/<suite_id>_<timestamp>/`.
In `--lean-graphs` and `--proof-lite` modes the bundle still keeps the paper-facing outputs:

- `index.html`: final dashboard with Paper Highlights, inline metric charts, 49-vs-64 comparisons, and run tables.
- `figure_exports/*.png`: polished paper figures for the main metrics plus the rest of the numeric suite metrics.
- `figure_exports/*.csv`: the same plotted data, ready to upload into Google Sheets.
- `figure_exports/*.tsv`: script-friendly plotted data.
- `google_sheets/*.csv`: all run rows, watched-node rows, metric averages, node-count summaries, and comparison tables for Google Sheets.

`--proof-lite` also keeps bounded node log files and slightly richer compact
evidence snippets, while still avoiding full raw evidence.

## Cross-Protocol Merge

If EGESS and Check-In are run on different computers, copy both `paper_reports/`
folders onto one machine and generate the final comparison page there:

```bash
python3 cross_protocol_summary.py \
  --egess-root /path/to/egess/paper_reports \
  --checkin-root /path/to/checkin/paper_reports
```

That produces:

- `comparison_reports/<timestamp>/index.html`
- `comparison_reports/<timestamp>/cross_protocol_overview.tsv`
- `comparison_reports/<timestamp>/combined_<scenario>.tsv`
- `comparison_reports/<timestamp>/figure_exports/*.png`
- `comparison_reports/<timestamp>/figure_exports/*.tsv`

## Statistical Analysis

After both teams finish collecting data, run the statistics post-processor from
the EGESS repo. It generates confidence intervals, percentiles, graph-ready TSVs,
paired t-tests, and mean-with-95%-CI PNG figures:

```bash
cd /Users/mustafa/egess
/Users/mustafa/egess/.venv/bin/python paper_eval_statistics.py \
  --egess-root /Users/mustafa/egess/paper_reports \
  --checkin-root /Users/mustafa/egess/external/checkin-egess-eval/paper_reports
```

Open the statistics dashboard:

```bash
cd /Users/mustafa/egess
STATS_DIR=$(ls -1dt statistics_reports/* | head -n 1)
open "$STATS_DIR/index.html"
```

The output includes:

- `metric_statistics.tsv`: sample mean, sample standard deviation, standard error, 95% confidence interval, and p50/p90/p95/p99.
- `overhead_percentiles.tsv`: overhead-focused percentiles for run-level and watched-node bytes/MB.
- `paired_t_tests.tsv`: paired EGESS vs Check-In comparisons matched by scenario, node count, run index, and seed.
- `boxplot_data.tsv`: median, quartiles, whiskers, and outlier counts.
- `cdf_points.tsv`: CDF-ready values for detection latency, throughput counters, and overhead.
- `histogram_bins.tsv`: histogram-ready distribution bins.
- `figure_exports/`: paper-ready PNG figures and their supporting TSVs.
