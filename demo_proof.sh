#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

NODES=16
DURATION_SEC=60
BASE_PORT=9000
KEEP_RUNNING=0
SCENARIO="firebomb"
TRIGGER_INTERVAL=3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes)
      NODES="$2"
      shift 2
      ;;
    --duration)
      DURATION_SEC="$2"
      shift 2
      ;;
    --base-port)
      BASE_PORT="$2"
      shift 2
      ;;
    --keep-running)
      KEEP_RUNNING=1
      shift
      ;;
    --scenario)
      SCENARIO="$2"
      shift 2
      ;;
    --trigger-interval)
      TRIGGER_INTERVAL="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: $0 [--nodes N] [--duration SEC] [--base-port PORT] [--trigger-interval SEC] [--scenario firebomb] [--keep-running]"
      exit 1
      ;;
  esac
done

if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python)"
else
  echo "ERROR: No Python interpreter found."
  exit 1
fi

if ! [[ "$NODES" =~ ^[0-9]+$ ]] || [[ "$NODES" -lt 2 ]]; then
  echo "ERROR: --nodes must be an integer >= 2"
  exit 1
fi

if ! [[ "$DURATION_SEC" =~ ^[0-9]+$ ]] || [[ "$DURATION_SEC" -lt 12 ]]; then
  echo "ERROR: --duration must be an integer >= 12"
  exit 1
fi

if ! [[ "$BASE_PORT" =~ ^[0-9]+$ ]]; then
  echo "ERROR: --base-port must be an integer"
  exit 1
fi

if ! [[ "$TRIGGER_INTERVAL" =~ ^[0-9]+$ ]] || [[ "$TRIGGER_INTERVAL" -lt 1 ]]; then
  echo "ERROR: --trigger-interval must be an integer >= 1"
  exit 1
fi

if [[ "$SCENARIO" != "firebomb" ]]; then
  echo "ERROR: --scenario currently supports only 'firebomb'"
  exit 1
fi

cleanup() {
  if [[ "$KEEP_RUNNING" -eq 0 ]]; then
    ./stop_nodes.sh >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "Stopping any existing nodes..."
./stop_nodes.sh >/dev/null 2>&1 || true

echo "Starting $NODES nodes with EGESS_LOG=1..."
EGESS_LOG=1 ./start_nodes.sh "$NODES"
RUN_DIR="$(ls -1dt runs/* | head -n 1)"
if [[ -z "$RUN_DIR" ]]; then
  echo "ERROR: Could not determine run directory."
  exit 1
fi

EVENTS_FILE="$RUN_DIR/demo_events.jsonl"
EVIDENCE_FILE="$RUN_DIR/protocol_evidence.json"
SUMMARY_FILE="$RUN_DIR/protocol_summary.txt"

echo "Run dir: $RUN_DIR"
echo "Waiting for listeners to become reachable..."
"$PYTHON_BIN" - "$BASE_PORT" "$NODES" <<'PY'
import json
import sys
import time
from urllib import request

base = int(sys.argv[1])
n = int(sys.argv[2])

def pull_once(port: int) -> bool:
    payload = {"op": "pull", "data": {}, "metadata": {"origin": "bootstrap"}}
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        f"http://127.0.0.1:{port}/",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=0.8) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return bool(body.get("op") == "receipt")
    except Exception:
        return False

deadline = time.time() + 35.0
while time.time() < deadline:
    ok = 0
    for p in range(base, base + n):
        if pull_once(p):
            ok += 1
    if ok >= n:
        print(f"All {n} nodes reachable.")
        sys.exit(0)
    time.sleep(1.0)

print("Timeout waiting for nodes.")
sys.exit(1)
PY

echo "Running automatic protocol scenario ($SCENARIO) for ${DURATION_SEC}s..."
"$PYTHON_BIN" - "$BASE_PORT" "$NODES" "$DURATION_SEC" "$TRIGGER_INTERVAL" "$EVENTS_FILE" <<'PY'
import json
import sys
import time
from urllib import request

base = int(sys.argv[1])
n = int(sys.argv[2])
duration = int(sys.argv[3])
trigger_interval = int(sys.argv[4])
events_file = sys.argv[5]

ports = list(range(base, base + n))
p0 = ports[n // 2]
p1 = ports[min(n - 1, (n // 2) + 1)]
p2 = ports[min(n - 1, (n // 2) + 2)]

def post(port: int, payload: dict, timeout: float = 1.2):
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        f"http://127.0.0.1:{port}/",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def log_event(kind: str, data: dict):
    row = {
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "kind": kind,
        "data": data,
    }
    with open(events_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")

def set_fault(port: int, fault: str, enable: bool, period_sec: int = 4):
    payload = {
        "op": "inject_fault",
        "data": {"fault": fault, "enable": bool(enable), "period_sec": int(period_sec)},
        "metadata": {"origin": "demo_proof"},
    }
    try:
        res = post(port, payload, timeout=1.2)
        ok = bool(res.get("data", {}).get("success", False))
        log_event("fault", {"port": port, "fault": fault, "enable": enable, "ok": ok, "response": res})
    except Exception as e:
        log_event("fault_error", {"port": port, "fault": fault, "enable": enable, "error": str(e)})

def trigger_push(port: int, label: str):
    payload = {
        "op": "push",
        "data": {"type": "demo_push", "label": label, "ts": int(time.time())},
        "metadata": {"origin": port, "relay": 0, "forward_count": 0},
    }
    try:
        res = post(port, payload, timeout=1.2)
        ok = bool(res.get("data", {}).get("success", False))
        log_event("trigger", {"port": port, "label": label, "ok": ok, "response": res})
    except Exception as e:
        log_event("trigger_error", {"port": port, "label": label, "error": str(e)})

for p in (p0, p1, p2):
    set_fault(p, "crash_sim", False)
    set_fault(p, "lie_sensor", False)
    set_fault(p, "flap", False)

start = time.time()
stage = -1
next_trigger = start
trigger_idx = 0
segment = max(3.0, float(duration) / 4.0)

while True:
    now = time.time()
    elapsed = now - start
    if elapsed >= duration:
        break

    new_stage = int(elapsed // segment)
    if new_stage > 3:
        new_stage = 3

    if new_stage != stage:
        stage = new_stage
        if stage == 0:
            log_event("stage", {"name": "baseline"})
        elif stage == 1:
            set_fault(p0, "lie_sensor", True)
            set_fault(p1, "flap", True)
            log_event("stage", {"name": "fire_disagreement", "ports": [p0, p1]})
        elif stage == 2:
            set_fault(p2, "crash_sim", True)
            log_event("stage", {"name": "bomb_crash", "port": p2})
        elif stage == 3:
            set_fault(p0, "lie_sensor", False)
            set_fault(p1, "flap", False)
            set_fault(p2, "crash_sim", False)
            log_event("stage", {"name": "recovery"})

    if now >= next_trigger:
        tp = ports[trigger_idx % len(ports)]
        trigger_push(tp, f"stage_{stage}_idx_{trigger_idx}")
        trigger_idx += 1
        next_trigger = now + float(trigger_interval)

    time.sleep(0.4)

for p in (p0, p1, p2):
    set_fault(p, "crash_sim", False)
    set_fault(p, "lie_sensor", False)
    set_fault(p, "flap", False)

log_event("done", {"duration_sec": duration, "trigger_count": trigger_idx})
PY

echo "Collecting protocol evidence..."
"$PYTHON_BIN" - "$BASE_PORT" "$NODES" "$EVENTS_FILE" "$EVIDENCE_FILE" "$SUMMARY_FILE" "$RUN_DIR" <<'PY'
import json
import sys
from urllib import request

base = int(sys.argv[1])
n = int(sys.argv[2])
events_file = sys.argv[3]
evidence_file = sys.argv[4]
summary_file = sys.argv[5]
run_dir = sys.argv[6]

def post(port: int, payload: dict, timeout: float = 1.0):
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        f"http://127.0.0.1:{port}/",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

totals = {
    "pull_rx": 0,
    "push_rx": 0,
    "pull_tx": 0,
    "push_tx": 0,
    "tx_ok": 0,
    "tx_fail": 0,
    "tx_timeout": 0,
    "tx_conn_error": 0,
}
nodes = {}
reachable = 0

for p in range(base, base + n):
    payload = {"op": "pull", "data": {}, "metadata": {"origin": "protocol_evidence"}}
    try:
        res = post(p, payload, timeout=1.0)
        state = res.get("data", {}).get("node_state", {})
        counters = state.get("msg_counters", {})
        if not isinstance(counters, dict):
            counters = {}
        for k in totals:
            totals[k] += int(counters.get(k, 0))
        nodes[str(p)] = {
            "reachable": True,
            "local_reading": state.get("local_reading"),
            "accepted_messages": int(state.get("accepted_messages", 0)),
            "known_nodes_count": len(state.get("known_nodes", [])) if isinstance(state.get("known_nodes"), list) else 0,
            "dfa_state": state.get("dfa_state"),
            "boundary_kind": state.get("boundary_kind"),
            "score": state.get("score"),
            "destroy_score": state.get("destroy_score"),
            "spread_score": state.get("spread_score"),
            "boundary_edges": state.get("boundary_edges", []),
            "destroy_state": state.get("destroy_state"),
            "spread_state": state.get("spread_state"),
            "destroy_edges": state.get("destroy_edges", []),
            "spread_edges": state.get("spread_edges", []),
            "faults": state.get("faults", {}),
            "msg_counters": counters,
            "recent_msgs_tail": state.get("recent_msgs", [])[-8:] if isinstance(state.get("recent_msgs"), list) else [],
        }
        reachable += 1
    except Exception as e:
        nodes[str(p)] = {"reachable": False, "error": str(e)}

event_count = 0
stage_changes = 0
fault_ops = 0
trigger_ops = 0
try:
    with open(events_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event_count += 1
            row = json.loads(line)
            kind = row.get("kind", "")
            if kind == "stage":
                stage_changes += 1
            elif kind in ("fault", "fault_error"):
                fault_ops += 1
            elif kind in ("trigger", "trigger_error"):
                trigger_ops += 1
except FileNotFoundError:
    pass

evidence = {
    "run_dir": run_dir,
    "reachable_nodes": reachable,
    "total_nodes": n,
    "totals": totals,
    "event_counts": {
        "events_total": event_count,
        "stage_changes": stage_changes,
        "fault_ops": fault_ops,
        "trigger_ops": trigger_ops,
    },
    "nodes": nodes,
}

with open(evidence_file, "w", encoding="utf-8") as f:
    json.dump(evidence, f, indent=2)

lines = [
    f"Run directory: {run_dir}",
    f"Reachable nodes: {reachable}/{n}",
    f"Events: total={event_count} stages={stage_changes} faults={fault_ops} triggers={trigger_ops}",
    "Traffic totals:",
    f"  rx pull/push: {totals['pull_rx']}/{totals['push_rx']}",
    f"  tx pull/push: {totals['pull_tx']}/{totals['push_tx']}",
    f"  tx ok/fail/timeout/conn_error: {totals['tx_ok']}/{totals['tx_fail']}/{totals['tx_timeout']}/{totals['tx_conn_error']}",
    f"Evidence JSON: {evidence_file}",
]
with open(summary_file, "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")

print("\n".join(lines))
PY

echo ""
echo "Done."
echo "Events:   $EVENTS_FILE"
echo "Evidence: $EVIDENCE_FILE"
echo "Summary:  $SUMMARY_FILE"

if [[ "$KEEP_RUNNING" -eq 1 ]]; then
  echo "Nodes are still running (--keep-running)."
else
  echo "Nodes have been stopped."
fi
