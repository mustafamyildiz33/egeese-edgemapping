#!/usr/bin/env python3
"""
EGESS Side-by-Side Visualizer (Live)

LEFT  = Gossip/Network view (known_nodes edges)
RIGHT = Hex Map (honeycomb) with:
        - DFA state coloring (00 missing, 01 stable, 10 verifying, 11 boundary) when available
        - T/instability score shown INSIDE each hex
        - Click-to-zoom inspector (neighbors, missing, disagreement)

Works with your current EGESS:
- Pulls node_state from each node via POST {"op":"pull"} to "/"
- Does not require CSV
"""

import argparse
import json
import math
import time
import textwrap
from urllib import request

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon

DEFAULT_ENDPOINTS = ["/"]

# Prevent focus stealing on some backends (notably macOS) during live redraws.
matplotlib.rcParams["figure.raise_window"] = False


# -------------------------
# HTTP helpers
# -------------------------

def post_json(url: str, payload: dict, timeout: float = 0.75):
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw)


def pull_node_state(port: int, endpoints):
    payload = {"op": "pull", "data": {}, "metadata": {}}
    last_err = None
    for ep in endpoints:
        url = f"http://127.0.0.1:{port}{ep}"
        try:
            res = post_json(url, payload)
            if isinstance(res, dict) and res.get("op") == "receipt":
                data = res.get("data", {})
                if isinstance(data, dict) and "node_state" in data:
                    return data["node_state"], ep
        except Exception as e:
            last_err = e
            continue
    return None, last_err


def inject_fault(port: int, fault: str, enable: bool = True, period_sec: int = 4):
    payload = {
        "op": "inject_fault",
        "data": {
            "fault": str(fault),
            "enable": bool(enable),
            "period_sec": int(period_sec),
        },
        "metadata": {},
    }
    try:
        res = post_json(f"http://127.0.0.1:{port}/", payload, timeout=1.2)
        data = res.get("data", {}) if isinstance(res, dict) else {}
        ok = bool(data.get("success", False))
        return ok, data
    except Exception as e:
        return False, {"message": str(e)}


def send_demo_push(port: int, label: str = "autodemo"):
    payload = {
        "op": "push",
        "data": {
            "type": "autodemo",
            "label": str(label),
            "ts": int(time.time()),
        },
        "metadata": {
            "origin": int(port),
            "relay": 0,
            "forward_count": 0,
        },
    }
    try:
        res = post_json(f"http://127.0.0.1:{port}/", payload, timeout=1.2)
        data = res.get("data", {}) if isinstance(res, dict) else {}
        return bool(data.get("success", False)), data
    except Exception as e:
        return False, {"message": str(e)}


def center_ports(base_port: int, n: int, grid: int, k: int = 4):
    pts = []
    cx = (grid - 1) / 2.0
    cy = (grid - 1) / 2.0
    for idx in range(n):
        p = base_port + idx
        r, c = port_to_rc(base_port, p, grid)
        d2 = (r - cy) ** 2 + (c - cx) ** 2
        pts.append((d2, p))
    pts.sort(key=lambda x: (x[0], x[1]))
    return [p for _, p in pts[:max(1, k)]]


def neighbor_slots(base_port: int, grid: int, port: int):
    """
    Return up to 6 oriented neighbor slots for a given port.
    Slots are 1-based and stable by geometric direction.
    """
    r, c = port_to_rc(base_port, port, grid)
    out = []
    slot = 1
    for nr, nc in hex_neighbors_rc(r, c):
        np = rc_to_port(base_port, nr, nc, grid)
        if np is not None:
            out.append((slot, np))
        slot += 1
    return out


# -------------------------
# Hex geometry + neighbor math
# -------------------------

def port_to_rc(base_port: int, port: int, grid: int):
    idx = port - base_port
    r = idx // grid
    c = idx % grid
    return r, c


def rc_to_port(base_port: int, r: int, c: int, grid: int):
    if r < 0 or c < 0 or r >= grid or c >= grid:
        return None
    return base_port + (r * grid + c)


def hex_neighbors_rc(r: int, c: int):
    """
    Odd-r offset neighbors (pointy-top hex, rows shifted horizontally).
    This matches the typical "honeycomb" look where odd rows are shifted right.
    """
    if r % 2 == 0:
        deltas = [(-1, 0), (-1, -1),
                  (0, -1), (0, 1),
                  (1, 0), (1, -1)]
    else:
        deltas = [(-1, 1), (-1, 0),
                  (0, -1), (0, 1),
                  (1, 1), (1, 0)]
    return [(r + dr, c + dc) for dr, dc in deltas]


def hex_center_xy(r: int, c: int, size: float):
    """
    Pointy-top hex layout with odd rows shifted right.
    """
    # horizontal spacing between centers
    x = size * math.sqrt(3) * (c + (0.5 if (r % 2 == 1) else 0.0))
    # vertical spacing between centers
    y = size * 1.5 * r
    return x, y


def hex_corners(xc: float, yc: float, size: float):
    """
    Pointy-top hex corners (6 points).
    """
    pts = []
    for i in range(6):
        angle = math.radians(60 * i - 30)  # -30 => pointy-top orientation
        x = xc + size * math.cos(angle)
        y = yc + size * math.sin(angle)
        pts.append((x, y))
    return pts


# -------------------------
# DFA + Score utilities
# -------------------------

def get_dfa_bits(node_state):
    """
    Try a few likely keys. Returns int in {0,1,2,3} or None.
    """
    if not isinstance(node_state, dict):
        return None

    for k in ("dfa_state_bits", "dfa_state", "state_bits", "state"):
        v = node_state.get(k, None)
        if isinstance(v, int) and 0 <= v <= 3:
            return v
        if isinstance(v, str):
            # allow "00","01","10","11"
            if v in ("00", "01", "10", "11"):
                return int(v, 2)
    return None


def score_bucket(score: int):
    """
    2-bit bucket:
      00 Cold (0-1)
      01 Warm (2-3)
      10 Hot  (4-5)
      11 Boil (>=6)
    """
    if score <= 1:
        return 0
    if score <= 3:
        return 1
    if score <= 5:
        return 2
    return 3


def compute_local_score(base_port, grid, port, states_by_port, manual_disagree=None):
    """
    Visualization-side scoring if nodes don't store it yet.

    +3 if neighbor is missing/unreachable
    +1 if neighbor disagrees on local_reading
    """
    r, c = port_to_rc(base_port, port, grid)
    me = states_by_port.get(port, {})
    my_reading = me.get("local_reading", "BLUE")

    score = 0
    missing_neighbors = []
    disagree_neighbors = []
    manual_set = set()
    if isinstance(manual_disagree, dict):
        vals = manual_disagree.get(port, [])
        if isinstance(vals, (list, set, tuple)):
            manual_set = set(int(x) for x in vals if isinstance(x, int))

    for nr, nc in hex_neighbors_rc(r, c):
        np = rc_to_port(base_port, nr, nc, grid)
        if np is None:
            continue

        nst = states_by_port.get(np, None)
        if nst is None:
            score += 3
            missing_neighbors.append(np)
            continue

        n_reading = nst.get("local_reading", "BLUE")
        if n_reading != my_reading:
            score += 1
            disagree_neighbors.append(np)
            continue

        if np in manual_set:
            score += 1
            disagree_neighbors.append(np)

    return score, missing_neighbors, disagree_neighbors


def score_trend(delta):
    if delta is None:
        return "new"
    if delta > 0:
        return "increasing (+{})".format(delta)
    if delta < 0:
        return "decreasing ({})".format(delta)
    return "steady (0)"


def build_score_snapshot(base_port, n, grid, states_by_port, prev_scores, manual_disagree=None):
    analysis = {}
    next_scores = {}
    for idx in range(n):
        p = base_port + idx
        st = states_by_port.get(p, None)
        if st is None:
            analysis[p] = {
                "score": 0,
                "missing_neighbors": [],
                "disagree_neighbors": [],
                "delta": None,
                "trend": "offline",
            }
            continue

        score, miss_n, dis_n = compute_local_score(base_port, grid, p, states_by_port, manual_disagree)
        old_score = prev_scores.get(p, None)
        delta = None if old_score is None else int(score) - int(old_score)
        manual_set = set()
        if isinstance(manual_disagree, dict):
            vals = manual_disagree.get(p, [])
            if isinstance(vals, (list, set, tuple)):
                manual_set = set(int(x) for x in vals if isinstance(x, int))
        analysis[p] = {
            "score": int(score),
            "missing_neighbors": miss_n,
            "disagree_neighbors": dis_n,
            "manual_disagree_neighbors": sorted(list(manual_set)),
            "delta": delta,
            "trend": score_trend(delta),
        }
        next_scores[p] = int(score)

    prev_scores.clear()
    prev_scores.update(next_scores)
    return analysis


def dfa_style(bits: int):
    """
    Color per 2-bit DFA state.
    """
    # 00 Missing
    if bits == 0:
        return ("#bdbdbd", "black")
    # 01 Stable
    if bits == 1:
        return ("#2f6fb3", "black")
    # 10 Verifying
    if bits == 2:
        return ("#f0b54d", "black")
    # 11 Boundary
    return ("#d94b4b", "black")


def fallback_style(node_state):
    """
    If DFA bits not present, use local_reading for rough visuals.
    """
    lr = "BLUE"
    if isinstance(node_state, dict):
        lr = node_state.get("local_reading", "BLUE")

    if lr == "RED":
        return ("#d94b4b", "black")
    return ("#2f6fb3", "black")


# -------------------------
# Drawing
# -------------------------

def draw_gossip(ax, base_port, n, grid, size, states_by_port, title_suffix):
    ax.clear()
    ax.set_title(
        "EGESS Gossip View (Live) | " + title_suffix,
        fontsize=10
    )

    # positions at hex centers
    pos = {}
    for idx in range(n):
        p = base_port + idx
        r, c = port_to_rc(base_port, p, grid)
        pos[p] = hex_center_xy(r, c, size)

    # edge weights
    edge_weights = {}
    for p, st in states_by_port.items():
        known = st.get("known_nodes", [])
        if not isinstance(known, list):
            continue
        for k in known:
            if not isinstance(k, int):
                continue
            if k < base_port or k >= base_port + n:
                continue
            a, b = sorted((p, k))
            edge_weights[(a, b)] = edge_weights.get((a, b), 0) + 1

    # mutual boost
    for (a, b) in list(edge_weights.keys()):
        a_knows = states_by_port.get(a, {}).get("known_nodes", [])
        b_knows = states_by_port.get(b, {}).get("known_nodes", [])
        if isinstance(a_knows, list) and isinstance(b_knows, list):
            if (b in a_knows) and (a in b_knows):
                edge_weights[(a, b)] += 2

    # draw edges
    for (a, b), w in edge_weights.items():
        if a not in pos or b not in pos:
            continue
        x1, y1 = pos[a]
        x2, y2 = pos[b]
        lw = 0.6 + 0.35 * min(w, 8)
        ax.plot([x1, x2], [y1, y2], linewidth=lw, alpha=0.28)

    # nodes as hex outlines
    centers = []
    for idx in range(n):
        p = base_port + idx
        r, c = port_to_rc(base_port, p, grid)
        xc, yc = pos[p]
        centers.append((p, xc, yc))
        missing = p not in states_by_port

        if missing:
            face, _ = ("#bdbdbd", "black")
        else:
            bits = get_dfa_bits(states_by_port[p])
            if bits is None:
                face, _ = fallback_style(states_by_port[p])
            else:
                face, _ = dfa_style(bits)

        poly = Polygon(hex_corners(xc, yc, size * 0.92), closed=True, facecolor=face, edgecolor="black", linewidth=0.6)
        ax.add_patch(poly)
        ax.text(xc, yc, str(p), ha="center", va="center", fontsize=7, color="black")

    ax.set_aspect("equal", adjustable="box")
    ax.set_xticks([])
    ax.set_yticks([])
    reset_zoom(ax, base_port, n, grid, size)
    return centers


def draw_hex_map(ax, base_port, n, grid, size, states_by_port, analysis_by_port, title_suffix, inspector_state, controls_hint):
    ax.clear()
    ax.set_title(
        "EGESS Hex Map (honeycomb) | " + title_suffix + "\n"
        "Fill changes by T threshold (stable->yellow->red), outline red=disagree, yellow=missing-neighbor",
        fontsize=10
    )

    missing_ports = []
    patches = []
    pos = {}

    # draw all hexes
    for idx in range(n):
        p = base_port + idx
        r, c = port_to_rc(base_port, p, grid)
        xc, yc = hex_center_xy(r, c, size)
        pos[p] = (xc, yc)

        st = states_by_port.get(p, None)
        if st is None:
            missing_ports.append(p)
            face, tcol = dfa_style(0)
            score = int(analysis_by_port.get(p, {}).get("score", 0))
            delta = analysis_by_port.get(p, {}).get("delta", None)
            outline = "#000000"
            miss_n = []
            dis_n = []
            manual_dis_n = []
        else:
            bits = get_dfa_bits(st)
            if bits is None:
                face, tcol = fallback_style(st)
            else:
                face, tcol = dfa_style(bits)

            info = analysis_by_port.get(p, {})
            score = int(info.get("score", 0))
            delta = info.get("delta", None)
            miss_n = info.get("missing_neighbors", [])
            dis_n = info.get("disagree_neighbors", [])
            manual_dis_n = info.get("manual_disagree_neighbors", [])
            bucket = score_bucket(score)
            T_high = int(st.get("T_high", 4)) if isinstance(st, dict) else 4

            # Outline priority:
            # 1) disagreement with at least one neighbor => red
            # 2) missing/unreachable neighbor(s) => yellow
            # 3) otherwise use mild score bucket shading
            if len(dis_n) > 0:
                outline = "#d94b4b"
            elif len(miss_n) > 0:
                outline = "#f0b54d"
            else:
                outline = ["#1a1a1a", "#2c7fb8", "#2c7fb8", "#2c7fb8"][bucket]

            # Threshold-based fill overrides for stable/unknown nodes.
            # Makes T transitions visible even before DFA boundary confirmation.
            if bits in (None, 1):
                if score >= T_high + 2:
                    face = "#d94b4b"
                elif score >= T_high:
                    face = "#f0b54d"

        poly = Polygon(hex_corners(xc, yc, size), closed=True, facecolor=face, edgecolor=outline, linewidth=2.0 if st is not None else 1.3)
        ax.add_patch(poly)
        patches.append((p, poly, (xc, yc)))

        # Port label small top
        ax.text(xc, yc - size * 0.42, str(p), ha="center", va="center", fontsize=6, color="black")

        # Score in center
        ax.text(xc, yc + size * 0.05, str(score), ha="center", va="center", fontsize=10, color="black", fontweight="bold")
        if delta is not None:
            if delta > 0:
                d_text = "+{}".format(delta)
                d_color = "#d94b4b"
            elif delta < 0:
                d_text = str(delta)
                d_color = "#2c7fb8"
            else:
                d_text = "0"
                d_color = "#444444"
            ax.text(xc, yc + size * 0.32, "dT {}".format(d_text), ha="center", va="center", fontsize=6, color=d_color)
        if st is not None and len(manual_dis_n) > 0:
            ax.text(xc, yc + size * 0.48, "m+{}".format(len(manual_dis_n)), ha="center", va="center", fontsize=6, color="#d94b4b")

        # If missing, big X
        if st is None:
            ax.plot([xc - size * 0.45, xc + size * 0.45], [yc - size * 0.45, yc + size * 0.45], linewidth=2.0, color="black")
            ax.plot([xc + size * 0.45, xc - size * 0.45], [yc - size * 0.45, yc + size * 0.45], linewidth=2.0, color="black")
    # For selected node, explicitly draw disagreement relations so they're visible.
    sel = inspector_state.get("selected_port")
    if sel is not None and sel in pos and sel in analysis_by_port:
        sx, sy = pos[sel]
        sel_info = analysis_by_port.get(sel, {})
        for np in sel_info.get("disagree_neighbors", []):
            if np in pos:
                tx, ty = pos[np]
                ax.plot([sx, tx], [sy, ty], linewidth=3.0, color="#d94b4b", alpha=0.9)
        for np in sel_info.get("missing_neighbors", []):
            if np in pos:
                tx, ty = pos[np]
                ax.plot([sx, tx], [sy, ty], linewidth=2.2, color="#f0b54d", alpha=0.9, linestyle="--")
        ring = Polygon(hex_corners(sx, sy, size * 1.08), closed=True, fill=False, edgecolor="#111111", linewidth=2.6)
        ax.add_patch(ring)

    ax.set_aspect("equal", adjustable="box")
    ax.set_xticks([])
    ax.set_yticks([])
    # Keep a consistent full-map framing unless inspector zoom overrides later.
    reset_zoom(ax, base_port, n, grid, size)

    return patches


# -------------------------
# Interaction
# -------------------------

def build_inspector_text(base_port, grid, port, states_by_port, analysis_by_port):
    st = states_by_port.get(port, None)
    info = analysis_by_port.get(port, {})
    score = int(info.get("score", 0))
    miss_n = info.get("missing_neighbors", [])
    dis_n = info.get("disagree_neighbors", [])
    manual_dis_n = info.get("manual_disagree_neighbors", [])
    delta = info.get("delta", None)
    trend = info.get("trend", "n/a")
    bucket = score_bucket(score)

    bits = None
    if st is not None:
        bits = get_dfa_bits(st)

    r, c = port_to_rc(base_port, port, grid)
    neigh_ports = []
    for nr, nc in hex_neighbors_rc(r, c):
        np = rc_to_port(base_port, nr, nc, grid)
        if np is not None:
            neigh_ports.append(np)

    slots = neighbor_slots(base_port, grid, port)
    slot_text = []
    for slot, np in slots:
        slot_text.append("{}->{}".format(slot, np))

    counters = {}
    recent = []
    faults = {}
    t_high = 4
    t_low = 2
    if isinstance(st, dict):
        raw_counters = st.get("msg_counters", {})
        if isinstance(raw_counters, dict):
            counters = raw_counters
        raw_recent = st.get("recent_msgs", [])
        if isinstance(raw_recent, list):
            recent = raw_recent[-6:]
        raw_faults = st.get("faults", {})
        if isinstance(raw_faults, dict):
            faults = raw_faults
        try:
            t_high = int(st.get("T_high", 4))
            t_low = int(st.get("T_low", 2))
        except Exception:
            t_high = 4
            t_low = 2

    recent_text = "(none)"
    if len(recent) > 0:
        recent_text = "\n - ".join(recent)

    def bits_str(b):
        if b is None:
            return "n/a"
        return format(int(b), "02b")

    return (
        f"Node: {port}\n"
        f"Grid: r={r} c={c}\n"
        f"DFA bits: {bits_str(bits)}\n"
        f"T score: {score}   bucket: {bits_str(bucket)}\n"
        f"T thresholds: low={t_low} high={t_high}\n"
        f"T trend: {trend}\n"
        f"T drivers: +3*missing ({len(miss_n)}) + +1*disagree ({len(dis_n)})\n"
        f"Manual disagree neighbors: {manual_dis_n}\n"
        f"T delta: {'n/a' if delta is None else delta}\n"
        f"Neighbor slots: {slot_text}\n"
        f"Neighbors: {neigh_ports}\n"
        f"Missing neighbors: {miss_n}\n"
        f"Disagree neighbors: {dis_n}\n"
        f"Faults crash/lie/flap: {bool(faults.get('crash_sim', False))}/{bool(faults.get('lie_sensor', False))}/{bool(faults.get('flap', False))}\n"
        f"Traffic rx(pull/push): {int(counters.get('pull_rx', 0))}/{int(counters.get('push_rx', 0))}\n"
        f"Traffic tx(pull/push): {int(counters.get('pull_tx', 0))}/{int(counters.get('push_tx', 0))}\n"
        f"Traffic tx(ok/fail/timeout): {int(counters.get('tx_ok', 0))}/{int(counters.get('tx_fail', 0))}/{int(counters.get('tx_timeout', 0))}\n"
        f"Recent msgs:\n - {recent_text}\n"
    )


def aggregate_traffic(states_by_port):
    total = {
        "pull_rx": 0,
        "push_rx": 0,
        "pull_tx": 0,
        "push_tx": 0,
        "tx_ok": 0,
        "tx_fail": 0,
        "tx_timeout": 0,
    }
    for st in states_by_port.values():
        if not isinstance(st, dict):
            continue
        counters = st.get("msg_counters", {})
        if not isinstance(counters, dict):
            continue
        for k in list(total.keys()):
            try:
                total[k] += int(counters.get(k, 0))
            except Exception:
                pass
    return total


def draw_info_panel(ax, inspector_state, status_line, controls_hint):
    ax.clear()
    ax.set_title("Inspector / Demo Controls", fontsize=10, loc="left")
    ax.axis("off")

    y = 0.97
    ax.text(0.01, y, status_line, transform=ax.transAxes, va="top", ha="left", fontsize=9)
    y -= 0.14
    ax.text(0.01, y, controls_hint, transform=ax.transAxes, va="top", ha="left", fontsize=9)
    y -= 0.16

    if inspector_state.get("active", False):
        raw = inspector_state.get("text", "")
        wrapped_lines = []
        for line in str(raw).splitlines():
            wrapped = textwrap.wrap(line, width=92) or [""]
            wrapped_lines.extend(wrapped)
        text = "\n".join(wrapped_lines[:24])
    else:
        text = "Click a node on left/right map to inspect and zoom."

    ax.text(
        0.01,
        y,
        text,
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=9,
        family="monospace",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor="black", alpha=0.9),
    )


def zoom_to_port(ax, base_port, grid, port, size, pad=2.4):
    r, c = port_to_rc(base_port, port, grid)
    xc, yc = hex_center_xy(r, c, size)
    span = size * pad * 3.0
    ax.set_xlim(xc - span, xc + span)
    ax.set_ylim(yc + span, yc - span)  # invert y for "map feel"


def reset_zoom(ax, base_port, n, grid, size):
    # compute bounds from all centers
    xs = []
    ys = []
    for idx in range(n):
        p = base_port + idx
        r, c = port_to_rc(base_port, p, grid)
        x, y = hex_center_xy(r, c, size)
        xs.append(x)
        ys.append(y)
    margin = size * 2.2
    ax.set_xlim(min(xs) - margin, max(xs) + margin)
    ax.set_ylim(max(ys) + margin, min(ys) - margin)


# -------------------------
# Main loop
# -------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-port", type=int, default=9000)
    ap.add_argument("--n", type=int, default=64)
    ap.add_argument("--grid", type=int, default=8)
    ap.add_argument("--fps", type=float, default=2.0, help="Refresh rate (frames per second)")
    ap.add_argument("--hex-size", type=float, default=1.0, help="Hex radius in plot units")
    ap.add_argument("--auto-demo", type=str, default="off", choices=["off", "firebomb"], help="Automatic no-click demo scenario")
    ap.add_argument("--auto-period", type=float, default=10.0, help="Seconds per auto-demo stage")
    args = ap.parse_args()

    base_port = args.base_port
    n = args.n
    grid = args.grid
    size = args.hex_size

    endpoints = DEFAULT_ENDPOINTS

    fig = plt.figure(figsize=(15.5, 8.8))
    gs = plt.GridSpec(2, 2, width_ratios=[1, 1], height_ratios=[1, 0.34], wspace=0.06, hspace=0.08)
    ax_left = plt.subplot(gs[0, 0])
    ax_right = plt.subplot(gs[0, 1])
    ax_info = plt.subplot(gs[1, :])

    inspector_state = {"active": False, "text": "", "selected_port": None, "zoomed": False}
    status_state = {"text": "", "ts": 0.0}
    last_patches = []
    left_centers = []
    score_history = {}
    manual_disagree = {}
    auto_state = {
        "enabled": args.auto_demo != "off",
        "start_ts": time.time(),
        "stage": -1,
        "last_trigger_ts": 0.0,
        "ports": center_ports(base_port, n, grid, k=4),
    }

    def set_status(message):
        status_state["text"] = str(message)
        status_state["ts"] = time.time()

    def apply_auto_stage(stage):
        if len(auto_state["ports"]) == 0:
            return
        p0 = int(auto_state["ports"][0])
        p1 = int(auto_state["ports"][1]) if len(auto_state["ports"]) > 1 else p0
        p2 = int(auto_state["ports"][2]) if len(auto_state["ports"]) > 2 else p1

        for p in auto_state["ports"]:
            inject_fault(int(p), "crash_sim", False)
            inject_fault(int(p), "lie_sensor", False)
            inject_fault(int(p), "flap", False)

        if stage == 0:
            set_status("auto-demo: baseline")
        elif stage == 1:
            inject_fault(p0, "lie_sensor", True)
            inject_fault(p1, "flap", True)
            set_status("auto-demo: fire/disagreement on core nodes")
        elif stage == 2:
            inject_fault(p2, "crash_sim", True)
            set_status("auto-demo: bomb/crash on node {}".format(p2))
        elif stage == 3:
            inject_fault(p0, "lie_sensor", False)
            inject_fault(p1, "flap", False)
            inject_fault(p2, "crash_sim", False)
            set_status("auto-demo: recovery")

    def clear_inspector():
        inspector_state["active"] = False
        inspector_state["text"] = ""
        inspector_state["selected_port"] = None
        inspector_state["zoomed"] = False
        reset_zoom(ax_right, base_port, n, grid, size)
        plt.draw()

    def on_click(event):
        nonlocal inspector_state
        if event.inaxes not in (ax_right, ax_left):
            return
        if event.button in (2, 3):
            clear_inspector()
            return
        x, y = event.xdata, event.ydata
        if x is None or y is None:
            return

        # find nearest hex center
        best_port = None
        best_d = None
        if event.inaxes == ax_right:
            candidates = [(p, xc, yc) for p, _, (xc, yc) in last_patches]
        else:
            candidates = left_centers
        for p, xc, yc in candidates:
            d = (x - xc) ** 2 + (y - yc) ** 2
            if best_d is None or d < best_d:
                best_d = d
                best_port = p

        # Ignore clicks that are far from any hex center (prevents accidental selection/zoom).
        if best_port is None or best_d is None or best_d > (size * 1.2) ** 2:
            return

        inspector_state["active"] = True
        inspector_state["selected_port"] = best_port
        inspector_state["text"] = build_inspector_text(base_port, grid, best_port, states_by_port_global, analysis_global)
        inspector_state["zoomed"] = True
        zoom_to_port(ax_right, base_port, grid, best_port, size)
        plt.draw()

    def on_key(event):
        if event.key in ("q", "Q"):
            plt.close("all")
            return
        if event.key in ("0",):
            port = inspector_state.get("selected_port")
            if port is not None:
                manual_disagree[int(port)] = set()
                set_status("manual disagree cleared for {}".format(port))
            return
        if event.key in ("1", "2", "3", "4", "5", "6"):
            port = inspector_state.get("selected_port")
            if port is not None:
                slot = int(event.key)
                slots = neighbor_slots(base_port, grid, int(port))
                target = None
                for s, np in slots:
                    if int(s) == slot:
                        target = int(np)
                        break
                if target is None:
                    set_status("slot {} invalid for {}".format(slot, port))
                    return
                if int(port) not in manual_disagree or not isinstance(manual_disagree[int(port)], set):
                    manual_disagree[int(port)] = set()
                if target in manual_disagree[int(port)]:
                    manual_disagree[int(port)].remove(target)
                    set_status("manual disagree OFF: {} slot {} -> {}".format(port, slot, target))
                else:
                    manual_disagree[int(port)].add(target)
                    set_status("manual disagree ON: {} slot {} -> {}".format(port, slot, target))
            return
        if event.key in ("d", "D", "backspace", "delete"):
            port = inspector_state.get("selected_port")
            if port is not None:
                ok, data = inject_fault(int(port), "crash_sim", True)
                set_status("delete {} -> {}".format(port, "ok" if ok else "failed"))
            return
        if event.key in ("a", "A"):
            port = inspector_state.get("selected_port")
            if port is not None:
                ok, data = inject_fault(int(port), "crash_sim", False)
                set_status("add {} -> {}".format(port, "ok" if ok else "failed"))
            return
        if event.key in ("l", "L"):
            port = inspector_state.get("selected_port")
            if port is not None:
                st = states_by_port_global.get(int(port), {})
                faults = st.get("faults", {}) if isinstance(st, dict) else {}
                current = bool(faults.get("lie_sensor", False)) if isinstance(faults, dict) else False
                ok, data = inject_fault(int(port), "lie_sensor", not current)
                set_status("lie_sensor {} -> {}".format(port, "on" if (not current and ok) else ("off" if (current and ok) else "failed")))
            return
        if event.key in ("f", "F"):
            port = inspector_state.get("selected_port")
            if port is not None:
                st = states_by_port_global.get(int(port), {})
                faults = st.get("faults", {}) if isinstance(st, dict) else {}
                current = bool(faults.get("flap", False)) if isinstance(faults, dict) else False
                ok, data = inject_fault(int(port), "flap", not current)
                set_status("flap {} -> {}".format(port, "on" if (not current and ok) else ("off" if (current and ok) else "failed")))
            return
        if event.key in ("z", "Z"):
            if inspector_state.get("selected_port") is not None:
                inspector_state["zoomed"] = True
                zoom_to_port(ax_right, base_port, grid, inspector_state["selected_port"], size)
                plt.draw()
            return
        if event.key in ("r", "R", "escape", "home"):
            clear_inspector()

    cid_click = plt.gcf().canvas.mpl_connect("button_press_event", on_click)
    cid_key = plt.gcf().canvas.mpl_connect("key_press_event", on_key)

    reset_zoom(ax_right, base_port, n, grid, size)

    # global shared state for inspector callback
    global states_by_port_global
    states_by_port_global = {}
    global analysis_global
    analysis_global = {}

    try:
        while True:
            now = time.time()
            if auto_state.get("enabled", False):
                stage_period = max(4.0, float(args.auto_period))
                stage = int((now - auto_state["start_ts"]) // stage_period) % 4
                if stage != auto_state.get("stage", -1):
                    apply_auto_stage(stage)
                    auto_state["stage"] = stage
                trigger_period = max(2.0, stage_period / 2.0)
                if now - auto_state.get("last_trigger_ts", 0.0) >= trigger_period and len(auto_state["ports"]) > 0:
                    tp = int(auto_state["ports"][stage % len(auto_state["ports"])])
                    ok, data = send_demo_push(tp, label="auto-stage-{}".format(stage))
                    auto_state["last_trigger_ts"] = now
                    if ok:
                        set_status("auto trigger -> {} (stage {})".format(tp, stage))

            states_by_port = {}
            ok_ep = None

            for idx in range(n):
                p = base_port + idx
                st, meta = pull_node_state(p, endpoints=endpoints)
                if st is not None:
                    states_by_port[p] = st
                    ok_ep = ok_ep or meta

            states_by_port_global = states_by_port
            analysis_by_port = build_score_snapshot(base_port, n, grid, states_by_port, score_history, manual_disagree)
            analysis_global = analysis_by_port

            up_ports = [p for p, info in analysis_by_port.items() if info.get("delta") is not None and info.get("delta") > 0]
            down_ports = [p for p, info in analysis_by_port.items() if info.get("delta") is not None and info.get("delta") < 0]
            totals = aggregate_traffic(states_by_port)
            trend_suffix = "T_up={} T_down={} | tx(p/s)={}/{} rx(p/s)={}/{} ok/fail={}/{}".format(
                len(up_ports),
                len(down_ports),
                totals["pull_tx"],
                totals["push_tx"],
                totals["pull_rx"],
                totals["push_rx"],
                totals["tx_ok"],
                totals["tx_fail"]
            )
            if time.time() - status_state["ts"] < 6.0:
                trend_suffix = trend_suffix + " | " + status_state["text"][:60]

            title_suffix = f"endpoint={ok_ep or 'unreachable'} | {time.strftime('%H:%M:%S')}"
            status_line = trend_suffix

            left_centers = draw_gossip(ax_left, base_port, n, grid, size, states_by_port, title_suffix)
            controls_hint = "Controls: click node (left/right)=select+zoom | 1..6 toggle disagree slot | 0 clear slots | d delete | a add | l lie_sensor | f flap | r reset | q quit"
            last_patches = draw_hex_map(ax_right, base_port, n, grid, size, states_by_port, analysis_by_port, title_suffix, inspector_state, controls_hint)
            draw_info_panel(ax_info, inspector_state, status_line, controls_hint)

            # if inspector is active and zoom was explicitly requested, keep zoom pinned
            if (
                inspector_state.get("active", False)
                and inspector_state.get("zoomed", False)
                and inspector_state.get("selected_port") is not None
            ):
                zoom_to_port(ax_right, base_port, grid, inspector_state["selected_port"], size)
                inspector_state["text"] = build_inspector_text(
                    base_port,
                    grid,
                    inspector_state["selected_port"],
                    states_by_port_global,
                    analysis_global
                )

            plt.pause(max(0.001, 1.0 / max(0.25, args.fps)))
    except KeyboardInterrupt:
        pass
    finally:
        plt.close("all")


if __name__ == "__main__":
    main()
