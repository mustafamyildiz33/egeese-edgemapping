# EGESS Paper Evaluation Lab Run Sheet

Use this sheet during the school-lab run. The goal is to collect 30 complete
batches without filling storage, while still keeping strong graphs, Google
Sheets CSVs, HTML dashboards, and export bundles for the paper.

## Best Mode

Use `--proof-lite`.

This is the best middle ground:

- Keeps final HTML dashboards.
- Keeps paper figure PNGs and graph data.
- Keeps Google Sheets CSVs.
- Keeps compact evidence snippets.
- Keeps small capped node logs at 32 KB per node.
- Avoids true `--full-logs` and `--full-evidence`.

Use `--lean-graphs` only if storage gets tight and you are willing to skip node
logs. Do not use `--data-only` for the paper run because it skips generated
graphs.

## Before Anyone Starts

Run this once from the EGESS repo:

```bash
cd /Users/mustafa/egess
./verify_eval_matrix.sh
df -h .
```

If multiple lab computers are on the same shared server, each person must use a
different `--base-port`. Do not let everyone use `9000`.

Recommended ports:

| Computer | EGESS base port | Check-In base port |
| --- | ---: | ---: |
| 1 | 9100 | 9200 |
| 2 | 9300 | 9400 |
| 3 | 9500 | 9600 |
| 4 | 9700 | 9800 |

Every 60-second case starts fresh. The graphs combine the results later, but the
protocol state does not carry from one 60-second case into the next.

## EGESS Protocol

Working folder:

```bash
cd /Users/mustafa/egess
```

Run the five parts below. After each part, run the checkpoint before starting
the next part.

| Part | Batch range | Run command |
| --- | --- | --- |
| 1 | 1-6 | `./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 6 --batch-start 1 --nodes 49,64 --proof-lite` |
| 2 | 7-12 | `./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 6 --batch-start 7 --nodes 49,64 --proof-lite` |
| 3 | 13-18 | `./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 6 --batch-start 13 --nodes 49,64 --proof-lite` |
| 4 | 19-24 | `./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 6 --batch-start 19 --nodes 49,64 --proof-lite` |
| 5 | 25-30 | `./run_paper_eval.sh --base-port 9100 --mode all --duration 60 --batches 6 --batch-start 25 --nodes 49,64 --proof-lite` |

Checkpoint after each EGESS part:

```bash
cd /Users/mustafa/egess
./.venv/bin/python check_chunk_status.py --base-port 9100 --batch-start 1 --batches 6 --nodes 49,64
```

Change `--batch-start` to match the part you just finished: `1`, `7`, `13`,
`19`, or `25`.

Open the latest EGESS chunk HTML:

```bash
CHUNK_DIR=$(ls -1dt /Users/mustafa/egess/campaign_reports/all_together_60s_*_p9100* | head -n 1)
open "$CHUNK_DIR/index.html"
```

After all five EGESS parts, merge and export:

```bash
cd /Users/mustafa/egess
./.venv/bin/python merge_paper_reports.py --base-port 9100 --nodes 49,64 --duration-sec 60 --expected-batches 30
MERGED_DIR=$(ls -1dt /Users/mustafa/egess/merged_paper_reports/merged_*_p9100* | head -n 1)
open "$MERGED_DIR/index.html"
```

In the merged HTML, click `Download Export Bundle`. That ZIP is the file to move
to a personal laptop.

## Check-In Protocol

Working folder:

```bash
cd /Users/mustafa/egess/external/checkin-egess-eval
```

Run the five parts below. After each part, run the checkpoint from the EGESS
repo before starting the next part.

| Part | Batch range | Run command |
| --- | --- | --- |
| 1 | 1-6 | `./run_paper_eval.sh --base-port 9200 --mode all --duration 60 --batches 6 --batch-start 1 --nodes 49,64 --proof-lite` |
| 2 | 7-12 | `./run_paper_eval.sh --base-port 9200 --mode all --duration 60 --batches 6 --batch-start 7 --nodes 49,64 --proof-lite` |
| 3 | 13-18 | `./run_paper_eval.sh --base-port 9200 --mode all --duration 60 --batches 6 --batch-start 13 --nodes 49,64 --proof-lite` |
| 4 | 19-24 | `./run_paper_eval.sh --base-port 9200 --mode all --duration 60 --batches 6 --batch-start 19 --nodes 49,64 --proof-lite` |
| 5 | 25-30 | `./run_paper_eval.sh --base-port 9200 --mode all --duration 60 --batches 6 --batch-start 25 --nodes 49,64 --proof-lite` |

Checkpoint after each Check-In part:

```bash
cd /Users/mustafa/egess
./.venv/bin/python check_chunk_status.py --root /Users/mustafa/egess/external/checkin-egess-eval --base-port 9200 --batch-start 1 --batches 6 --nodes 49,64
```

Change `--batch-start` to match the part you just finished: `1`, `7`, `13`,
`19`, or `25`.

Open the latest Check-In chunk HTML:

```bash
CHUNK_DIR=$(ls -1dt /Users/mustafa/egess/external/checkin-egess-eval/campaign_reports/all_together_60s_*_p9200* | head -n 1)
open "$CHUNK_DIR/index.html"
```

After all five Check-In parts, merge and export from the EGESS repo:

```bash
cd /Users/mustafa/egess
./.venv/bin/python merge_paper_reports.py --root checkin=/Users/mustafa/egess/external/checkin-egess-eval/paper_reports --base-port 9200 --nodes 49,64 --duration-sec 60 --expected-batches 30
MERGED_DIR=$(ls -1dt /Users/mustafa/egess/merged_paper_reports/merged_*_p9200* | head -n 1)
open "$MERGED_DIR/index.html"
```

In the merged HTML, click `Download Export Bundle`.

## Stop/Go Checklist After Each Part

Do not start the next part until the checker says `OK`.

Confirm:

- Row count is `48`.
- Batch range matches the part, such as `1-6` or `7-12`.
- Phases are `phase1,phase2,phase3,phase4`.
- Nodes are `49,64`.
- Seeds are the expected range and not repeated.
- No `FAILED`, `ERROR`, blank, or still-`RUNNING` rows.
- Scenario dashboards exist.
- Figure PNG/TSV data exists.
- Google Sheets CSVs exist.
- The HTML dashboard opens.

Look at the quick trends after each part:

- Does `64` nodes use more MB than `49` nodes?
- Are failures/timeouts staying reasonable?
- Is phase 4 showing stress without breaking the run?
- Are detection and recovery values non-empty where the phase should produce them?

If a checkpoint fails, stop there, save the terminal output, and rerun only that
part after fixing the issue.

## Files To Submit Or Move

Each protocol's final merged folder contains:

- `index.html`: final dashboard.
- `portable_export/combined_all_runs.csv`: main Google Sheets run table.
- `portable_export/combined_watch_nodes.csv`: watched-node evidence.
- `portable_export/combined_summary_by_nodes.csv`: 49-vs-64 comparison table.
- `portable_export/combined_metric_averages.csv`: graph metric averages.
- `figure_exports/`: paper figures and graph data inside each scenario folder.
- `*_portable_export.zip`: one-click export bundle.

The CSV run sheets live in:

- `paper_eval/run_sheets/egess_proof_lite_batch_sheet.csv`
- `paper_eval/run_sheets/checkin_proof_lite_batch_sheet.csv`
