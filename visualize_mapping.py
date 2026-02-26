#!/usr/bin/env python3
"""
EGESS Side-by-Side Visualizer (Live)

LEFT  = Gossip/Network view (who knows who; inferred communication fabric)
RIGHT = Grid Map view (8x8 field map: outside/inside/boundary/missing)

- Pulls node_state from each node via POST {"op":"pull"} to "/"
- Does NOT require CSV
- Does NOT modify professor files; visualization-only

Map states:
  BLUE   = outside
  RED    = inside
  ORANGE = boundary/disagreement band
  GRAY   = missing/unreachable
"""

import argparse
import json
import math
import time
from urllib import request

import matplotlib.pyplot as plt


# EGESS nodes listen on "/" for POST in your node.py
DEFAULT_ENDPOINTS = ["/"]


# --- HTTP helpers ---

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


# --- Layout ---

def grid_coords(base_port: int, n: int, grid: int):
    """
    Ports mapped row-major into a grid x grid.
    port = base_port + idx
    idx = r*grid + c
    coords are (c, r) with r increasing downward for map clarity.
    """
    coords = {}
    for idx in range(n):
        p = base_port + idx
        r = idx // grid
        c = idx % grid
        coords[p] = (c, r)
    return coords


# --- Map state model (visualization-only) ---

def infer_cell_state(port: int, coords, grid: int, missing: bool):
    """
    Research-friendly default if nodes don't provide a sensor reading:
    - Use distance-to-center to create an "inside" core and a boundary band.
    - This matches your “red inside, orange boundary, blue outside” expectation.
    """
    if missing:
        return "MISSING"

    x, y = coords[port]
    cx = (grid - 1) / 2.0
    cy = (grid - 1) / 2.0
    d = math.sqrt((x - cx) ** 2 + (y - cy) ** 2)

    # Tune these for different shapes
    inside_r = 1.2
    boundary_r = 2.4

    if d <= inside_r:
        return "INSIDE"
    if d <= boundary_r:
        return "BOUNDARY"
    return "OUTSIDE"


def state_to_style(state: str):
    """
    Returns (facecolor, textcolor).
    """
    if state == "INSIDE":
        return ("#d94b4b", "black")      # red-ish
    if state == "BOUNDARY":
        return ("#f0b54d", "black")      # orange-ish
    if state == "OUTSIDE":
        return ("#2f6fb3", "black")      # blue-ish
    return ("#bdbdbd", "black")          # gray for missing/unknown


# --- Drawing ---

def draw_map(ax, coords, states_by_port, base_port, n, grid, title_suffix):
    ax.clear()
    ax.set_title(f"EGESS Grid Map ({grid}x{grid})  |  {title_suffix}\n"
                 f"BLUE=outside  RED=inside  ORANGE=boundary(disagreement)  GRAY=missing",
                 fontsize=10)

    # Draw grid cells
    missing_ports = []
    for idx in range(n):
        p = base_port + idx
        x, y = coords[p]
        missing = p not in states_by_port
        if missing:
            missing_ports.append(p)

        cell_state = infer_cell_state(p, coords, grid, missing)
        face, tcol = state_to_style(cell_state)

        # Rectangle cell
        ax.add_patch(plt.Rectangle((x, y), 1, 1, fill=True, facecolor=face, edgecolor="black", linewidth=0.35))

        # Port label
        ax.text(x + 0.5, y + 0.52, str(p), ha="center", va="center", fontsize=7, color=tcol)

        # Missing mark
        if missing:
            ax.plot([x + 0.2, x + 0.8], [y + 0.2, y + 0.8], linewidth=1.2, color="black")
            ax.plot([x + 0.8, x + 0.2], [y + 0.2, y + 0.8], linewidth=1.2, color="black")

    ax.set_xlim(0, grid)
    ax.set_ylim(grid, 0)  # invert y for "map" feel (top row is 0)
    ax.set_aspect("equal", adjustable="box")
    ax.set_xticks([])
    ax.set_yticks([])

    ax.text(0.02, -0.06, f"Missing: {missing_ports[:18]}{'...' if len(missing_ports) > 18 else ''}",
            transform=ax.transAxes, fontsize=9, va="top")


def draw_gossip(ax, coords, states_by_port, base_port, n, grid, title_suffix):
    ax.clear()
    ax.set_title(f"EGESS Gossip View (Live)  |  {title_suffix}\n"
                 f"Edges from known_nodes (thicker = mutual/stronger)",
                 fontsize=10)

    # Node positions centered in each cell
    pos = {}
    for idx in range(n):
        p = base_port + idx
        x, y = coords[p]
        pos[p] = (x + 0.5, y + 0.5)

    # Build edges with weights
    # weight += 1 if A mentions B; +1 more if mutual
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

    # Add mutual boost
    # If both directions exist, we want to thicken more.
    # We approximate mutuality by checking if both A lists B and B lists A.
    for (a, b) in list(edge_weights.keys()):
        a_knows = states_by_port.get(a, {}).get("known_nodes", [])
        b_knows = states_by_port.get(b, {}).get("known_nodes", [])
        if isinstance(a_knows, list) and isinstance(b_knows, list):
            if (b in a_knows) and (a in b_knows):
                edge_weights[(a, b)] += 2

    # Draw edges
    for (a, b), w in edge_weights.items():
        if a not in pos or b not in pos:
            continue
        x1, y1 = pos[a]
        x2, y2 = pos[b]
        lw = 0.6 + 0.35 * min(w, 8)
        ax.plot([x1, x2], [y1, y2], linewidth=lw, alpha=0.28)

    # Draw nodes (circle markers), colored by inferred map state to visually connect layers
    for idx in range(n):
        p = base_port + idx
        x, y = pos[p]
        missing = p not in states_by_port
        cell_state = infer_cell_state(p, coords, grid, missing)
        face, _ = state_to_style(cell_state)

        if missing:
            ax.scatter([x], [y], s=160, marker="x")
        else:
            ax.scatter([x], [y], s=180, marker="o", edgecolors="black", linewidths=0.5, c=[face])

        ax.text(x, y - 0.38, str(p), ha="center", va="top", fontsize=7)

    ax.set_xlim(0, grid)
    ax.set_ylim(grid, 0)
    ax.set_aspect("equal", adjustable="box")
    ax.set_xticks([])
    ax.set_yticks([])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-port", type=int, default=9000)
    ap.add_argument("--n", type=int, default=64)
    ap.add_argument("--grid", type=int, default=8)
    ap.add_argument("--fps", type=float, default=2.0, help="Refresh rate (frames per second)")
    args = ap.parse_args()

    base_port = args.base_port
    n = args.n
    grid = args.grid

    endpoints = DEFAULT_ENDPOINTS

    coords = grid_coords(base_port, n, grid)

    plt.figure(figsize=(13.5, 6.8))
    gs = plt.GridSpec(1, 2, width_ratios=[1, 1], wspace=0.08)
    ax_left = plt.subplot(gs[0, 0])
    ax_right = plt.subplot(gs[0, 1])

    while True:
        states_by_port = {}
        ok_ep = None

        for idx in range(n):
            p = base_port + idx
            st, meta = pull_node_state(p, endpoints=endpoints)
            if st is not None:
                states_by_port[p] = st
                ok_ep = ok_ep or meta

        title_suffix = f"endpoint={ok_ep or 'unreachable'}  |  {time.strftime('%H:%M:%S')}"

        draw_gossip(ax_left, coords, states_by_port, base_port, n, grid, title_suffix)
        draw_map(ax_right, coords, states_by_port, base_port, n, grid, title_suffix)

        plt.pause(max(0.001, 1.0 / max(0.25, args.fps)))


if __name__ == "__main__":
    main()
