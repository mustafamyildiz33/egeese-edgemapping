#!/usr/bin/env python3
"""
EGESS Side-by-Side Visualizer (Live)

LEFT  = Gossip / network view
RIGHT = Hex map with protocol-state coloring, T score, and directional front cues

Works with the current EGESS runtime:
- Pulls `node_state` from each node via POST {"op":"pull"} to "/"
- Supports manual state injection for `NORMAL`, `ALERT`, and `RECOVERING`
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

DEFAULT_SCORE_MODEL = {
    "T_high": 8,
    "T_low": 2,
}


def _auto_grid_size(n: int) -> int:
    root = int(math.isqrt(int(n)))
    if root > 0 and root * root == int(n):
        return int(root)
    root = int(math.ceil(math.sqrt(float(n))))
    if root < 2:
        root = 2
    return int(root)


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


def inject_state(port: int, sensor_state: str):
    payload = {
        "op": "inject_state",
        "data": {"sensor_state": str(sensor_state).strip().upper()},
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


def resolve_score_model(states_by_port):
    model = dict(DEFAULT_SCORE_MODEL)
    sample_state = None
    for st in states_by_port.values():
        if isinstance(st, dict):
            sample_state = st
            break
    if isinstance(sample_state, dict):
        model["T_high"] = _to_int(sample_state.get("T_high", model["T_high"]), model["T_high"])
        model["T_low"] = _to_int(sample_state.get("T_low", model["T_low"]), model["T_low"])

    model["T_high"] = max(2, int(model["T_high"]))
    model["T_low"] = max(0, min(int(model["T_low"]), int(model["T_high"]) - 1))
    return model


def score_bucket(score: int, score_model: dict):
    t_low = int(score_model.get("T_low", DEFAULT_SCORE_MODEL["T_low"]))
    t_high = int(score_model.get("T_high", DEFAULT_SCORE_MODEL["T_high"]))
    warm_max = max(t_low + 1, t_high - 1)
    hot_max = t_high + 2
    if score <= t_low:
        return 0
    if score <= warm_max:
        return 1
    if score <= hot_max:
        return 2
    return 3


def score_trend(delta):
    if delta is None:
        return "new"
    if delta > 0:
        return "increasing (+{})".format(delta)
    if delta < 0:
        return "decreasing ({})".format(delta)
    return "steady (0)"


def build_score_snapshot(base_port, n, grid, states_by_port, prev_scores, prev_readings, score_model):
    analysis = {}
    next_scores = {}
    for idx in range(n):
        p = base_port + idx
        st = states_by_port.get(p, None)
        if st is None:
            analysis[p] = {
                "score": 0.0,
                "raw_score": 0.0,
                "front_score": 0.0,
                "impact_score": 0.0,
                "arrest_score": 0.0,
                "coherence_score": 0,
                "protocol_state": "MISSING",
                "dominant_sector": 0,
                "active_sectors": [],
                "front_score_by_sector": {},
                "missing_neighbors": [],
                "recovered_neighbors": [],
                "components": {"front": {}, "impact": {}, "coherence": {}, "arrest": {}},
                "delta": None,
                "trend": "offline",
            }
            continue

        score = round(_to_float(st.get("score", 0.0), 0.0), 1)
        old_score = prev_scores.get(p, None)
        delta = None if old_score is None else round(float(score) - float(old_score), 1)

        missing_neighbors = st.get("current_missing_neighbors", [])
        if not isinstance(missing_neighbors, list):
            missing_neighbors = []
        recovered_neighbors = st.get("recovered_neighbors", [])
        if not isinstance(recovered_neighbors, list):
            recovered_neighbors = []
        active_sectors = st.get("active_sectors", [])
        if not isinstance(active_sectors, list):
            active_sectors = []
        front_by_sector = st.get("front_score_by_sector", {})
        if not isinstance(front_by_sector, dict):
            front_by_sector = {}

        analysis[p] = {
            "score": float(score),
            "raw_score": round(_to_float(st.get("raw_score", score), score), 1),
            "front_score": round(_to_float(st.get("front_score", 0.0), 0.0), 1),
            "impact_score": round(_to_float(st.get("impact_score", 0.0), 0.0), 1),
            "arrest_score": round(_to_float(st.get("arrest_score", 0.0), 0.0), 1),
            "coherence_score": _to_int(st.get("coherence_score", 0), 0),
            "protocol_state": str(st.get("protocol_state", "NORMAL")),
            "dominant_sector": _to_int(st.get("dominant_sector", 0), 0),
            "active_sectors": [int(x) for x in active_sectors if isinstance(x, int)],
            "front_score_by_sector": {str(k): round(_to_float(v, 0.0), 1) for k, v in front_by_sector.items()},
            "missing_neighbors": sorted(int(x) for x in missing_neighbors if isinstance(x, int)),
            "recovered_neighbors": sorted(int(x) for x in recovered_neighbors if isinstance(x, int)),
            "components": {
                "front": st.get("front_components", {}),
                "impact": st.get("impact_components", {}),
                "coherence": st.get("coherence_components", {}),
                "arrest": st.get("arrest_components", {}),
            },
            "delta": delta,
            "trend": str(st.get("score_trend", score_trend(delta))),
        }
        next_scores[p] = float(score)

    prev_scores.clear()
    prev_scores.update(next_scores)
    prev_readings.clear()
    return analysis


def protocol_state_style(protocol_state, node_state=None, offline=False):
    if offline:
        return ("#bdbdbd", "black")

    state = str(protocol_state).strip().upper()
    palette = {
        "NORMAL": ("#2f6fb3", "black"),
        "WATCH": ("#6ecfe5", "black"),
        "WARNING": ("#f2c84b", "black"),
        "IMPACT": ("#e36a42", "black"),
        "STALLED": ("#9168d6", "white"),
        "CONTAINED": ("#6b4bb5", "white"),
        "RECOVERING": ("#63b46c", "black"),
        "MISSING": ("#bdbdbd", "black"),
    }
    if state in palette:
        return palette[state]

    sensor_state = ""
    if isinstance(node_state, dict):
        sensor_state = str(node_state.get("sensor_state", "")).strip().upper()
    if sensor_state == "ALERT":
        return palette["IMPACT"]
    if sensor_state == "RECOVERING":
        return palette["RECOVERING"]
    return palette["NORMAL"]


def dominant_lane_outline(info):
    return "#1a1a1a"


def hex_fill_style(protocol_state, score, score_model, offline=False):
    if offline:
        return "#bdbdbd", "black"

    state = str(protocol_state).strip().upper()
    if state == "RECOVERING":
        return "#63b46c", "black"
    if state in ("STALLED", "CONTAINED"):
        return ("#9168d6", "white") if state == "STALLED" else ("#6b4bb5", "white")
    if state in ("WATCH", "WARNING"):
        return "#f2c84b", "black"
    if state == "IMPACT":
        return "#e36a42", "black"
    return "#2f6fb3", "black"


def graph_neighbors(base_port: int, n: int, grid: int, port: int):
    r, c = port_to_rc(base_port, port, grid)
    out = []
    max_port = base_port + int(n)
    for nr, nc in hex_neighbors_rc(r, c):
        np = rc_to_port(base_port, nr, nc, grid)
        if np is None:
            continue
        if base_port <= int(np) < max_port:
            out.append(int(np))
    return out


def bfs_layers(base_port: int, n: int, grid: int, origin_port: int):
    max_port = base_port + int(n)
    origin = int(origin_port)
    if origin < base_port or origin >= max_port:
        return []

    visited = {origin}
    frontier = [origin]
    layers = [[origin]]
    while frontier:
        next_frontier = []
        for port in frontier:
            for neighbor in graph_neighbors(base_port, n, grid, port):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                next_frontier.append(int(neighbor))
        if len(next_frontier) == 0:
            break
        layers.append(sorted(next_frontier))
        frontier = next_frontier
    return layers


def corner_spread_layers(base_port: int, n: int, grid: int, max_layers: int = 6):
    layers = bfs_layers(base_port, n, grid, base_port)
    if max_layers <= 0:
        return layers
    return layers[:max_layers]


def center_strike_layers(base_port: int, n: int, grid: int):
    centers = center_ports(base_port, n, grid, k=1)
    if len(centers) == 0:
        return []

    center = int(centers[0])
    cr, cc = port_to_rc(base_port, center, grid)
    cx, cy = hex_center_xy(cr, cc, 1.0)

    candidates = []
    for idx in range(n):
        port = int(base_port + idx)
        if port == center:
            continue
        r, c = port_to_rc(base_port, port, grid)
        x, y = hex_center_xy(r, c, 1.0)
        vertical_bias = abs(float(x) - float(cx))
        radial = abs(float(y) - float(cy))
        candidates.append((vertical_bias, radial, port, y))

    above = [port for _, _, port, y in sorted(candidates) if y < cy]
    below = [port for _, _, port, y in sorted(candidates) if y > cy]

    layers = [[center]]
    shell_1 = []
    if len(above) > 0:
        shell_1.append(int(above[0]))
    if len(below) > 0:
        shell_1.append(int(below[0]))
    if len(shell_1) > 0:
        layers.append(sorted(list(dict.fromkeys(shell_1))))

    shell_2 = []
    if len(above) > 1:
        shell_2.append(int(above[1]))
    if len(below) > 1:
        shell_2.append(int(below[1]))
    if len(shell_2) > 0:
        layers.append(sorted(list(dict.fromkeys(shell_2))))

    return layers


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
            face, tcol = protocol_state_style("MISSING", offline=True)
        else:
            face, tcol = protocol_state_style(
                states_by_port[p].get("protocol_state", "NORMAL"),
                node_state=states_by_port[p],
            )

        poly = Polygon(hex_corners(xc, yc, size * 0.92), closed=True, facecolor=face, edgecolor="black", linewidth=0.6)
        ax.add_patch(poly)
        ax.text(xc, yc, str(p), ha="center", va="center", fontsize=7, color=tcol)

    ax.set_aspect("equal", adjustable="box")
    ax.set_xticks([])
    ax.set_yticks([])
    reset_zoom(ax, base_port, n, grid, size)
    return centers


def draw_hex_map(ax, base_port, n, grid, size, states_by_port, analysis_by_port, score_model, title_suffix, inspector_state, controls_hint):
    ax.clear()
    ax.set_title(
        "EGESS Hex Map (Live) | " + title_suffix + "\n"
        "Fill = state, center = T",
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
        info = analysis_by_port.get(p, {})
        if st is None:
            missing_ports.append(p)
            face, tcol = hex_fill_style("MISSING", 0.0, score_model, offline=True)
            score = round(_to_float(info.get("score", 0.0), 0.0), 1)
            delta = info.get("delta", None)
            outline = "#000000"
            miss_n = []
            recovered_n = []
            dominant_sector = 0
            protocol_state = "MISSING"
        else:
            protocol_state = str(info.get("protocol_state", st.get("protocol_state", "NORMAL")))
            score = round(_to_float(info.get("score", 0.0), 0.0), 1)
            face, tcol = hex_fill_style(protocol_state, score, score_model, offline=False)
            delta = info.get("delta", None)
            miss_n = info.get("missing_neighbors", [])
            recovered_n = info.get("recovered_neighbors", [])
            dominant_sector = _to_int(info.get("dominant_sector", 0), 0)
            outline = dominant_lane_outline(info)

        poly = Polygon(hex_corners(xc, yc, size), closed=True, facecolor=face, edgecolor=outline, linewidth=2.0 if st is not None else 1.3)
        ax.add_patch(poly)
        patches.append((p, poly, (xc, yc)))

        # Port label small top
        ax.text(xc, yc - size * 0.42, str(p), ha="center", va="center", fontsize=6, color=tcol)

        # Score in center
        ax.text(xc, yc + size * 0.08, "{:.1f}".format(score), ha="center", va="center", fontsize=11, color=tcol, fontweight="bold")

        # If missing, big X
        if st is None:
            ax.plot([xc - size * 0.45, xc + size * 0.45], [yc - size * 0.45, yc + size * 0.45], linewidth=2.0, color="black")
            ax.plot([xc + size * 0.45, xc - size * 0.45], [yc - size * 0.45, yc + size * 0.45], linewidth=2.0, color="black")

    # For selected node, explicitly draw disagreement relations so they're visible.
    sel = inspector_state.get("selected_port")
    if sel is not None and sel in pos and sel in analysis_by_port:
        sx, sy = pos[sel]
        sel_info = analysis_by_port.get(sel, {})
        for np in sel_info.get("missing_neighbors", []):
            if np in pos:
                tx, ty = pos[np]
                ax.plot([sx, tx], [sy, ty], linewidth=2.2, color="#f0b54d", alpha=0.9, linestyle="--")
        for np in sel_info.get("recovered_neighbors", []):
            if np in pos:
                tx, ty = pos[np]
                ax.plot([sx, tx], [sy, ty], linewidth=2.0, color="#2e7d32", alpha=0.9, linestyle=":")
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

def build_inspector_text(base_port, grid, port, states_by_port, analysis_by_port, score_model):
    st = states_by_port.get(port, None)
    info = analysis_by_port.get(port, {})
    score = round(_to_float(info.get("score", 0.0), 0.0), 1)
    raw_score = round(_to_float(info.get("raw_score", score), score), 1)
    front_score = round(_to_float(info.get("front_score", 0.0), 0.0), 1)
    impact_score = round(_to_float(info.get("impact_score", 0.0), 0.0), 1)
    arrest_score = round(_to_float(info.get("arrest_score", 0.0), 0.0), 1)
    coherence_score = _to_int(info.get("coherence_score", 0), 0)
    miss_n = info.get("missing_neighbors", [])
    rec_n = info.get("recovered_neighbors", [])
    delta = info.get("delta", None)
    trend = info.get("trend", "n/a")
    bucket = score_bucket(score, score_model)
    protocol_state = str(info.get("protocol_state", "NORMAL"))
    dominant_sector = _to_int(info.get("dominant_sector", 0), 0)
    active_sectors = info.get("active_sectors", [])
    if not isinstance(active_sectors, list):
        active_sectors = []
    front_by_sector = info.get("front_score_by_sector", {})
    if not isinstance(front_by_sector, dict):
        front_by_sector = {}

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
    boundary_kind = "stable"
    recent_alerts = []
    t_high = int(score_model.get("T_high", DEFAULT_SCORE_MODEL["T_high"]))
    t_low = int(score_model.get("T_low", DEFAULT_SCORE_MODEL["T_low"]))
    components = info.get("components", {})
    if not isinstance(components, dict):
        components = {}
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
        boundary_kind = str(st.get("boundary_kind", boundary_kind))
        raw_recent_alerts = st.get("recent_alerts", [])
        if isinstance(raw_recent_alerts, list):
            recent_alerts = raw_recent_alerts[-6:]
        try:
            t_high = int(st.get("T_high", t_high))
            t_low = int(st.get("T_low", t_low))
        except Exception:
            t_high = int(score_model.get("T_high", DEFAULT_SCORE_MODEL["T_high"]))
            t_low = int(score_model.get("T_low", DEFAULT_SCORE_MODEL["T_low"]))

    recent_text = "(none)"
    if len(recent) > 0:
        recent_text = "\n - ".join(recent)

    alerts_text = "(none)"
    if len(recent_alerts) > 0:
        alerts_text = ", ".join(str(x) for x in recent_alerts)

    def bits_str(b):
        if b is None:
            return "n/a"
        return format(int(b), "02b")

    return (
        f"Node: {port}\n"
        f"Grid: r={r} c={c}\n"
        f"Protocol state: {protocol_state}\n"
        f"DFA bits: {bits_str(bits)}\n"
        f"Boundary kind: {boundary_kind}\n"
        f"Pull cycles: {int(st.get('pull_cycles', 0)) if isinstance(st, dict) else 0}\n"
        f"T score: {score:.1f}   raw={raw_score:.1f}   bucket: {bits_str(bucket)}\n"
        f"T thresholds: low={t_low} high={t_high}\n"
        f"T trend: {trend}\n"
        f"Scores: front={front_score:.1f} impact={impact_score:.1f} arrest={arrest_score:.1f} coherence={coherence_score}\n"
        f"Dominant sector: {dominant_sector}   Active sectors: {active_sectors}\n"
        f"Front by sector: {front_by_sector}\n"
        f"No-progress cycles: {int(st.get('no_progress_cycles', 0)) if isinstance(st, dict) else 0}\n"
        f"T delta: {'n/a' if delta is None else delta}\n"
        f"Neighbor slots: {slot_text}\n"
        f"Neighbors: {neigh_ports}\n"
        f"Missing neighbors: {miss_n}\n"
        f"Recovered neighbors: {rec_n}\n"
        f"Front components: {components.get('front', {})}\n"
        f"Impact components: {components.get('impact', {})}\n"
        f"Coherence components: {components.get('coherence', {})}\n"
        f"Arrest components: {components.get('arrest', {})}\n"
        f"Sensor state/local reading: {str(st.get('sensor_state', 'NORMAL')) if isinstance(st, dict) else 'MISSING'}/{str(st.get('local_reading', 'n/a')) if isinstance(st, dict) else 'n/a'}\n"
        f"Faults crash/legacy: {bool(faults.get('crash_sim', False))}/{bool(faults.get('lie_sensor', False) or faults.get('flap', False))}\n"
        f"Traffic rx(pull/push): {int(counters.get('pull_rx', 0))}/{int(counters.get('push_rx', 0))}\n"
        f"Traffic tx(pull/push): {int(counters.get('pull_tx', 0))}/{int(counters.get('push_tx', 0))}\n"
        f"Traffic tx(ok/fail/timeout): {int(counters.get('tx_ok', 0))}/{int(counters.get('tx_fail', 0))}/{int(counters.get('tx_timeout', 0))}\n"
        f"Recent alerts: {alerts_text}\n"
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
    status_wrapped = textwrap.fill(str(status_line), width=165)
    ax.text(0.01, y, status_wrapped, transform=ax.transAxes, va="top", ha="left", fontsize=9)
    y -= 0.12
    controls_extended = "{} | i expand/collapse | j/k scroll details".format(controls_hint)
    controls_wrapped = textwrap.fill(str(controls_extended), width=165)
    ax.text(0.01, y, controls_wrapped, transform=ax.transAxes, va="top", ha="left", fontsize=8.7)
    y -= 0.14
    legend = "Colors: blue=normal, yellow=approach, red=impact, purple=stalled/contained, green=recovering, gray=missing"
    ax.text(0.01, y, textwrap.fill(legend, width=165), transform=ax.transAxes, va="top", ha="left", fontsize=8.5)
    y -= 0.10

    if inspector_state.get("active", False):
        raw = inspector_state.get("text", "")
        wrapped_lines = []
        for line in str(raw).splitlines():
            wrapped = textwrap.wrap(line, width=150) or [""]
            wrapped_lines.extend(wrapped)

        compact = bool(inspector_state.get("info_compact", True))
        max_lines = 8 if compact else 17
        offset = int(inspector_state.get("info_offset", 0))
        if offset < 0:
            offset = 0
        if offset > max(0, len(wrapped_lines) - 1):
            offset = max(0, len(wrapped_lines) - 1)

        page = wrapped_lines[offset: offset + max_lines]
        if len(page) == 0:
            page = ["(no data)"]

        tail = ""
        if compact and len(wrapped_lines) > max_lines:
            tail = "\n... press i for full details"
        elif (not compact) and (offset + max_lines < len(wrapped_lines)):
            tail = "\n... more below (j)"
        elif (not compact) and offset > 0:
            tail = "\n... more above (k)"

        text = "\n".join(page) + tail
    else:
        text = "Click a node on left/right map to inspect and zoom."

    ax.text(
        0.01,
        y,
        text,
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=8.4,
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
    ap.add_argument("--grid", type=int, default=0, help="Grid side length (0=auto)")
    ap.add_argument("--fps", type=float, default=2.0, help="Refresh rate (frames per second)")
    ap.add_argument("--hex-size", type=float, default=1.0, help="Hex radius in plot units")
    ap.add_argument(
        "--auto-demo",
        type=str,
        default="off",
        choices=["off", "firebomb", "spread", "tornado", "dual"],
        help="Automatic no-click demo scenario",
    )
    ap.add_argument("--auto-period", type=float, default=10.0, help="Seconds per auto-demo stage")
    ap.add_argument("--startup-grace", type=float, default=8.0, help="Seconds to suppress missing-neighbor penalty after visualizer start")
    args = ap.parse_args()

    base_port = args.base_port
    n = args.n
    grid = int(args.grid)
    if grid < 2:
        grid = _auto_grid_size(n)
    size = args.hex_size

    endpoints = DEFAULT_ENDPOINTS

    fig = plt.figure(figsize=(15.5, 8.8))
    gs = plt.GridSpec(2, 2, width_ratios=[1, 1], height_ratios=[1, 0.34], wspace=0.06, hspace=0.08)
    ax_left = plt.subplot(gs[0, 0])
    ax_right = plt.subplot(gs[0, 1])
    ax_info = plt.subplot(gs[1, :])

    inspector_state = {
        "active": False,
        "text": "",
        "selected_port": None,
        "zoomed": False,
        "info_compact": True,
        "info_offset": 0,
    }
    status_state = {"text": "", "ts": 0.0}
    last_patches = []
    left_centers = []
    score_history = {}
    reading_history = {}
    score_model = dict(DEFAULT_SCORE_MODEL)
    auto_state = {
        "enabled": args.auto_demo != "off",
        "mode": str(args.auto_demo),
        "start_ts": time.time(),
        "stage": -1,
        "last_trigger_ts": 0.0,
        "ports": center_ports(base_port, n, grid, k=4),
    }
    demo_state = {
        "mode": "off",
        "phase": "idle",
        "layers": [],
        "layer_index": 0,
        "recover_index": -1,
        "last_cycle_applied": -1,
        "step_period_cycles": 3,
        "active_alert_ports": [],
        "touched_ports": set(),
    }

    def all_ports():
        return [int(base_port + idx) for idx in range(n)]

    def set_status(message):
        status_state["text"] = str(message)
        status_state["ts"] = time.time()

    def reset_ports(ports, sensor_state="NORMAL"):
        for port in sorted(set(int(p) for p in ports)):
            inject_fault(int(port), "reset", False)
            inject_fault(int(port), "crash_sim", False)
            inject_state(int(port), sensor_state)

    def set_ports_alert(ports):
        for port in sorted(set(int(p) for p in ports)):
            inject_fault(int(port), "crash_sim", False)
            inject_state(int(port), "ALERT")

    def set_ports_missing(ports):
        for port in sorted(set(int(p) for p in ports)):
            inject_state(int(port), "NORMAL")
            inject_fault(int(port), "crash_sim", True)

    def set_ports_recovering(ports):
        for port in sorted(set(int(p) for p in ports)):
            inject_fault(int(port), "crash_sim", False)
            inject_state(int(port), "RECOVERING")

    def start_corner_demo(cycle_tick):
        layers = corner_spread_layers(base_port, n, grid)
        reset_ports(all_ports(), sensor_state="NORMAL")
        demo_state["mode"] = "corner-spread"
        demo_state["phase"] = "spreading"
        demo_state["layers"] = layers
        demo_state["layer_index"] = 0
        demo_state["recover_index"] = -1
        demo_state["last_cycle_applied"] = int(cycle_tick) - int(demo_state.get("step_period_cycles", 2))
        demo_state["active_alert_ports"] = []
        demo_state["touched_ports"] = set()
        set_status("demo corner-spread: armed at corner {}".format(base_port))

    def start_tornado_demo(cycle_tick):
        layers = center_strike_layers(base_port, n, grid)
        reset_ports(all_ports(), sensor_state="NORMAL")
        demo_state["mode"] = "tornado-mid"
        demo_state["phase"] = "striking"
        demo_state["layers"] = layers
        demo_state["layer_index"] = 0
        demo_state["recover_index"] = -1
        demo_state["last_cycle_applied"] = int(cycle_tick) - int(demo_state.get("step_period_cycles", 2))
        demo_state["active_alert_ports"] = []
        demo_state["touched_ports"] = set()
        set_status("demo tornado-mid: armed near center")

    def contain_demo():
        if demo_state.get("mode") != "corner-spread":
            set_status("contain ignored: no corner-spread demo active")
            return
        if demo_state.get("phase") != "spreading":
            set_status("contain ignored: demo phase={}".format(demo_state.get("phase")))
            return
        active_alert_ports = demo_state.get("active_alert_ports", [])
        if isinstance(active_alert_ports, list) and len(active_alert_ports) > 0:
            reset_ports(active_alert_ports, sensor_state="NORMAL")
            demo_state["active_alert_ports"] = []
        demo_state["phase"] = "contained"
        set_status("demo corner-spread: contain requested, waiting for stall/containment")

    def recover_demo(cycle_tick):
        if demo_state.get("mode") != "corner-spread":
            set_status("recover ignored: no corner-spread demo active")
            return
        if demo_state.get("layer_index", 0) <= 0:
            set_status("recover ignored: nothing has spread yet")
            return
        demo_state["phase"] = "recovering"
        demo_state["recover_index"] = int(demo_state.get("layer_index", 0)) - 1
        demo_state["last_cycle_applied"] = int(cycle_tick) - int(demo_state.get("step_period_cycles", 2))
        active_alert_ports = demo_state.get("active_alert_ports", [])
        if isinstance(active_alert_ports, list) and len(active_alert_ports) > 0:
            reset_ports(active_alert_ports, sensor_state="NORMAL")
            demo_state["active_alert_ports"] = []
        set_status("demo corner-spread: staged recovery started")

    def advance_demo(cycle_tick):
        if demo_state.get("mode") == "off":
            return
        step_period = max(1, int(demo_state.get("step_period_cycles", 2)))
        if int(cycle_tick) - int(demo_state.get("last_cycle_applied", -1)) < step_period:
            return

        mode = str(demo_state.get("mode", "off"))
        phase = str(demo_state.get("phase", "idle"))
        layers = demo_state.get("layers", [])
        if not isinstance(layers, list):
            layers = []

        if phase in ("spreading", "striking"):
            active_alert_ports = demo_state.get("active_alert_ports", [])
            if phase == "spreading" and isinstance(active_alert_ports, list) and len(active_alert_ports) > 0:
                set_ports_missing(active_alert_ports)
                for port in active_alert_ports:
                    demo_state["touched_ports"].add(int(port))

            idx = int(demo_state.get("layer_index", 0))
            if 0 <= idx < len(layers):
                ports = [int(p) for p in layers[idx]]
                if phase == "spreading":
                    set_ports_alert(ports)
                    demo_state["active_alert_ports"] = list(ports)
                else:
                    set_ports_missing(ports)
                    demo_state["active_alert_ports"] = []
                for port in ports:
                    demo_state["touched_ports"].add(int(port))
                demo_state["layer_index"] = idx + 1
                demo_state["last_cycle_applied"] = int(cycle_tick)
                label = "approach shell" if mode == "corner-spread" else "strike shell"
                set_status("demo {}: {} {} -> {}".format(mode, label, idx, ports))
            else:
                demo_state["phase"] = "holding" if mode == "tornado-mid" else "contained"
                demo_state["active_alert_ports"] = []
                demo_state["last_cycle_applied"] = int(cycle_tick)
                set_status("demo {}: no more new outages".format(mode))
            return

        if phase == "recovering":
            idx = int(demo_state.get("recover_index", -1))
            if idx >= 0 and idx < len(layers):
                ports = [int(p) for p in layers[idx]]
                set_ports_recovering(ports)
                for port in ports:
                    demo_state["touched_ports"].add(int(port))
                demo_state["recover_index"] = idx - 1
                demo_state["last_cycle_applied"] = int(cycle_tick)
                set_status("demo {}: recovery shell {} -> {}".format(mode, idx, ports))
            else:
                demo_state["phase"] = "recovered"
                demo_state["last_cycle_applied"] = int(cycle_tick)
                set_status("demo {}: recovery complete".format(mode))

    def apply_auto_stage(stage):
        mode = str(auto_state.get("mode", "firebomb"))
        if mode == "spread":
            if stage == 0:
                reset_ports(all_ports(), sensor_state="NORMAL")
                demo_state["mode"] = "off"
                demo_state["phase"] = "idle"
                demo_state["layers"] = []
                demo_state["layer_index"] = 0
                demo_state["recover_index"] = -1
                demo_state["active_alert_ports"] = []
                set_status("auto-demo(spread): baseline")
            elif stage == 1:
                start_corner_demo(-1)
                set_status("auto-demo(spread): start corner spread")
            elif stage == 2:
                contain_demo()
                set_status("auto-demo(spread): contain requested")
            elif stage == 3:
                recover_demo(-1)
                set_status("auto-demo(spread): recovery requested")
            return

        if mode == "tornado":
            if stage == 0:
                reset_ports(all_ports(), sensor_state="NORMAL")
                demo_state["mode"] = "off"
                set_status("auto-demo(tornado): baseline")
            elif stage == 1:
                start_tornado_demo(-1)
                set_status("auto-demo(tornado): middle strike start")
            elif stage == 2:
                set_status("auto-demo(tornado): holding strike")
            elif stage == 3:
                reset_ports(all_ports(), sensor_state="NORMAL")
                demo_state["mode"] = "off"
                set_status("auto-demo(tornado): reset to baseline")
            return

        if mode == "dual":
            if stage == 0:
                reset_ports(all_ports(), sensor_state="NORMAL")
                demo_state["mode"] = "off"
                set_status("auto-demo(dual): baseline")
            elif stage == 1:
                start_corner_demo(-1)
                set_status("auto-demo(dual): corner spread start")
            elif stage == 2:
                start_tornado_demo(-1)
                set_status("auto-demo(dual): tornado strike start")
            elif stage == 3:
                reset_ports(all_ports(), sensor_state="NORMAL")
                demo_state["mode"] = "off"
                set_status("auto-demo(dual): reset to baseline")
            return

        # firebomb default
        if stage == 0:
            reset_ports(all_ports(), sensor_state="NORMAL")
            demo_state["mode"] = "off"
            set_status("auto-demo(firebomb): baseline")
        elif stage == 1:
            start_corner_demo(-1)
            set_status("auto-demo(firebomb): front ignition")
        elif stage == 2:
            start_tornado_demo(-1)
            set_status("auto-demo(firebomb): center strike")
        elif stage == 3:
            recover_demo(-1)
            set_status("auto-demo(firebomb): recovery")

    def clear_inspector():
        inspector_state["active"] = False
        inspector_state["text"] = ""
        inspector_state["selected_port"] = None
        inspector_state["zoomed"] = False
        inspector_state["info_offset"] = 0
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
        inspector_state["text"] = build_inspector_text(
            base_port,
            grid,
            best_port,
            states_by_port_global,
            analysis_global,
            score_model,
        )
        inspector_state["zoomed"] = True
        inspector_state["info_offset"] = 0
        zoom_to_port(ax_right, base_port, grid, best_port, size)
        plt.draw()

    def on_key(event):
        if event.key in ("q", "Q"):
            plt.close("all")
            return
        if event.key in ("1",):
            cycle_tick = 0
            for st in states_by_port_global.values():
                if isinstance(st, dict):
                    cycle_tick = max(cycle_tick, _to_int(st.get("pull_cycles", 0), 0))
            start_corner_demo(cycle_tick)
            advance_demo(cycle_tick)
            return
        if event.key in ("2",):
            cycle_tick = 0
            for st in states_by_port_global.values():
                if isinstance(st, dict):
                    cycle_tick = max(cycle_tick, _to_int(st.get("pull_cycles", 0), 0))
            start_tornado_demo(cycle_tick)
            advance_demo(cycle_tick)
            return
        if event.key in ("c", "C"):
            contain_demo()
            return
        if event.key in ("v", "V"):
            cycle_tick = 0
            for st in states_by_port_global.values():
                if isinstance(st, dict):
                    cycle_tick = max(cycle_tick, _to_int(st.get("pull_cycles", 0), 0))
            recover_demo(cycle_tick)
            return
        if event.key in ("i", "I"):
            inspector_state["info_compact"] = not bool(inspector_state.get("info_compact", True))
            inspector_state["info_offset"] = 0
            plt.draw()
            return
        if event.key in ("j", "J", "down", "pagedown"):
            if not bool(inspector_state.get("info_compact", True)):
                inspector_state["info_offset"] = int(inspector_state.get("info_offset", 0)) + 4
                plt.draw()
            return
        if event.key in ("k", "K", "up", "pageup"):
            if not bool(inspector_state.get("info_compact", True)):
                inspector_state["info_offset"] = max(0, int(inspector_state.get("info_offset", 0)) - 4)
                plt.draw()
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
        if event.key in ("h", "H"):
            port = inspector_state.get("selected_port")
            if port is not None:
                ok, data = inject_state(int(port), "ALERT")
                set_status("sensor ALERT {} -> {}".format(port, "ok" if ok else "failed"))
            return
        if event.key in ("n", "N"):
            port = inspector_state.get("selected_port")
            if port is not None:
                ok, data = inject_state(int(port), "NORMAL")
                set_status("sensor NORMAL {} -> {}".format(port, "ok" if ok else "failed"))
            return
        if event.key in ("g", "G"):
            port = inspector_state.get("selected_port")
            if port is not None:
                ok, data = inject_state(int(port), "RECOVERING")
                set_status("sensor RECOVERING {} -> {}".format(port, "ok" if ok else "failed"))
            return
        if event.key in ("z", "Z"):
            if inspector_state.get("selected_port") is not None:
                inspector_state["zoomed"] = True
                zoom_to_port(ax_right, base_port, grid, inspector_state["selected_port"], size)
                plt.draw()
            return
        if event.key in ("r", "R", "escape", "home"):
            port = inspector_state.get("selected_port")
            if port is not None:
                inject_fault(int(port), "reset", False)
                inject_state(int(port), "NORMAL")
                set_status("reset {} -> baseline".format(port))
            clear_inspector()
            if event.key in ("R",):
                demo_state["mode"] = "off"
                demo_state["phase"] = "idle"
            return
        if event.key in ("x", "X"):
            reset_ports(all_ports(), sensor_state="NORMAL")
            demo_state["mode"] = "off"
            demo_state["phase"] = "idle"
            demo_state["layers"] = []
            demo_state["layer_index"] = 0
            demo_state["recover_index"] = -1
            demo_state["last_cycle_applied"] = -1
            demo_state["touched_ports"] = set()
            set_status("all nodes reset to baseline")
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

            # Auto-align visualizer grid with runtime node state to prevent false T from topology mismatch.
            detected_grid = None
            detected_set = set()
            for st in states_by_port.values():
                if isinstance(st, dict):
                    gs = st.get("grid_size", None)
                    if isinstance(gs, int) and gs >= 2:
                        detected_set.add(int(gs))
                    elif isinstance(gs, str) and gs.isdigit() and int(gs) >= 2:
                        detected_set.add(int(gs))
            if len(detected_set) == 1:
                detected_grid = list(detected_set)[0]
            if detected_grid is not None and detected_grid != grid:
                grid = int(detected_grid)
                auto_state["ports"] = center_ports(base_port, n, grid, k=4)
                set_status("grid auto-aligned to {}".format(grid))

            states_by_port_global = states_by_port
            cycle_tick = 0
            for st in states_by_port.values():
                if not isinstance(st, dict):
                    continue
                cycle_tick = max(cycle_tick, _to_int(st.get("pull_cycles", 0), 0))
            advance_demo(cycle_tick)
            score_model = resolve_score_model(states_by_port)
            analysis_by_port = build_score_snapshot(
                base_port,
                n,
                grid,
                states_by_port,
                score_history,
                reading_history,
                score_model,
            )
            analysis_global = analysis_by_port

            up_ports = [p for p, info in analysis_by_port.items() if info.get("delta") is not None and info.get("delta") > 0]
            down_ports = [p for p, info in analysis_by_port.items() if info.get("delta") is not None and info.get("delta") < 0]
            watch_count = 0
            warning_count = 0
            impact_count = 0
            stalled_count = 0
            contained_count = 0
            recovering_count = 0
            for st in states_by_port.values():
                if not isinstance(st, dict):
                    continue
                state = str(st.get("protocol_state", "NORMAL")).strip().upper()
                if state == "WATCH":
                    watch_count += 1
                elif state == "WARNING":
                    warning_count += 1
                elif state == "IMPACT":
                    impact_count += 1
                elif state == "STALLED":
                    stalled_count += 1
                elif state == "CONTAINED":
                    contained_count += 1
                elif state == "RECOVERING":
                    recovering_count += 1
            totals = aggregate_traffic(states_by_port)
            trend_suffix = "T_up={} T_down={} W={} Warn={} I={} Stall={} Cont={} Rec={} | tx(p/s)={}/{} rx(p/s)={}/{} ok/fail={}/{}".format(
                len(up_ports),
                len(down_ports),
                watch_count,
                warning_count,
                impact_count,
                stalled_count,
                contained_count,
                recovering_count,
                totals["pull_tx"],
                totals["push_tx"],
                totals["pull_rx"],
                totals["push_rx"],
                totals["tx_ok"],
                totals["tx_fail"]
            )
            trend_suffix = trend_suffix + " | T_low/high={}/{} | mode={}".format(
                int(score_model.get("T_low", DEFAULT_SCORE_MODEL["T_low"])),
                int(score_model.get("T_high", DEFAULT_SCORE_MODEL["T_high"])),
                str(args.auto_demo),
            )
            trend_suffix = trend_suffix + " | cycle={} demo={}/{}".format(
                int(cycle_tick),
                str(demo_state.get("mode", "off")),
                str(demo_state.get("phase", "idle")),
            )
            if time.time() - status_state["ts"] < 6.0:
                trend_suffix = trend_suffix + " | " + status_state["text"][:60]

            title_suffix = f"endpoint={ok_ep or 'unreachable'} | cycle={int(cycle_tick)} | {time.strftime('%H:%M:%S')}"
            status_line = trend_suffix

            left_centers = draw_gossip(ax_left, base_port, n, grid, size, states_by_port, title_suffix)
            controls_hint = "Controls: 1 corner-demo | c contain | v recover | 2 tornado-demo | x reset-all | click node=inspect | d delete | a add | h alert | n normal | g recovering | r reset-node | q quit"
            last_patches = draw_hex_map(
                ax_right,
                base_port,
                n,
                grid,
                size,
                states_by_port,
                analysis_by_port,
                score_model,
                title_suffix,
                inspector_state,
                controls_hint,
            )
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
                    analysis_global,
                    score_model,
                )

            plt.pause(max(0.001, 1.0 / max(0.25, args.fps)))
    except KeyboardInterrupt:
        pass
    finally:
        plt.close("all")


if __name__ == "__main__":
    main()
