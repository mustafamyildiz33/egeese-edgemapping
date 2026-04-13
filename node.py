
# EGESS - Experimental Gear for Evaluation of Swarm Systems
# Copyright (C) 2026  Nick Ivanov and ACSUS Lab <ivanov@rowan.edu>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


# -------------------------------------------------------------------------
# This is the main (starter) code for a node of a swarm network. It all
# starts here.
# -------------------------------------------------------------------------

# LIBRARY IMPORTS
import sys
import json
import os
import math
from flask import Flask, request, jsonify
import threading
import queue
import time

# LOCAL IMPORTS
import push_protocol
import background_protocol
import listener_protocol
import pull_protocol


def pull(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    while True:
        time.sleep(config_json["pull_period"])
        pull_protocol.pull_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue)


def push(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    while True:
        msg = push_queue.get()
        push_protocol.push_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue, msg)


def listener(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    app = Flask(__name__)

    @app.route("/", methods=['POST'])
    def egess_api():
        if not request.is_json:
            return jsonify({"error": "OOPS: Not JSON!"}), 400
        msg = request.get_json()
        return listener_protocol.listener_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue, msg)

    app.run(host=config_json["base_host"], port=this_port, debug=False, use_reloader=False, threaded=True)


def background(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    while True:
        time.sleep(config_json["background_period"])
        background_protocol.background_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue)


def _hex_neighbors_odd_r(col, row, grid):
    out = []
    if row % 2 == 0:
        candidates = [
            (col - 1, row), (col + 1, row),
            (col, row - 1), (col - 1, row - 1),
            (col, row + 1), (col - 1, row + 1),
        ]
    else:
        candidates = [
            (col - 1, row), (col + 1, row),
            (col + 1, row - 1), (col, row - 1),
            (col + 1, row + 1), (col, row + 1),
        ]
    for c, r in candidates:
        if 0 <= c < grid and 0 <= r < grid:
            out.append((c, r))
    return out


def _auto_grid_size(number_of_nodes):
    root = int(math.isqrt(int(number_of_nodes)))
    if root > 0 and root * root == int(number_of_nodes):
        return int(root)
    root = int(math.ceil(math.sqrt(float(number_of_nodes))))
    if root < 2:
        root = 2
    return int(root)


def main():
    if len(sys.argv) != 3:
        print("ERROR Two arguments expected.")
        print("USAGE: {} <port> <number_of_nodes>".format(sys.argv[0]))
        exit(1)

    config_file = "config.json"
    node_state_init_file = "node_state_init.json"

    this_port = int(sys.argv[1])
    number_of_nodes = int(sys.argv[2])

    with open(config_file) as file:
        config_json = json.load(file)

    env_base_host = os.environ.get("EGESS_BASE_HOST", "").strip()
    if env_base_host:
        config_json["base_host"] = env_base_host

    env_base_port = os.environ.get("EGESS_BASE_PORT", "").strip()
    if env_base_port.isdigit():
        config_json["base_port"] = int(env_base_port)

    with open(node_state_init_file) as file:
        node_state = json.load(file)

    env_grid = os.environ.get("EGESS_GRID_SIZE", "").strip()
    if env_grid.isdigit() and int(env_grid) >= 2:
        grid_size = int(env_grid)
    else:
        # Prefer an auto-fit square grid when possible (e.g., 36 -> 6x6).
        grid_size = int(node_state.get("grid_size", int(config_json.get("grid_size", _auto_grid_size(number_of_nodes)))))
        if grid_size * grid_size < int(number_of_nodes):
            grid_size = _auto_grid_size(number_of_nodes)

    base_port = int(config_json.get("base_port", 9000))

    idx = this_port - base_port
    gx = int(idx % grid_size)
    gy = int(idx // grid_size)

    node_state["grid_size"] = grid_size
    node_state["grid_pos"] = [gx, gy]
    node_state["started_ts"] = float(time.time())

    if gx == 0 or gx == grid_size - 1 or gy == 0 or gy == grid_size - 1:
        node_state["role"] = "sentinel"
    else:
        node_state["role"] = node_state.get("role", "normal")

    nbrs = []
    for (nc, nr) in _hex_neighbors_odd_r(gx, gy, grid_size):
        nidx = nr * grid_size + nc
        nport = base_port + nidx
        if base_port <= nport < base_port + number_of_nodes and nport != this_port:
            nbrs.append(int(nport))
    node_state["neighbors"] = sorted(list(set(nbrs)))

    node_state.setdefault("local_reading", "BLUE")
    node_state.setdefault("sensor_state", "NORMAL")
    node_state["T_high"] = float(config_json.get("T_high", float(node_state.get("T_high", 7))))
    node_state["T_low"] = float(config_json.get("T_low", float(node_state.get("T_low", 2))))
    node_state.setdefault("protocol_state", "NORMAL")
    node_state.setdefault("score", 0.0)
    node_state.setdefault("raw_score", 0.0)
    node_state.setdefault("score_delta", 0.0)
    node_state.setdefault("score_trend", "steady (0)")
    node_state.setdefault("score_bucket", 0)
    node_state.setdefault("front_score", 0.0)
    node_state.setdefault("impact_score", 0.0)
    node_state.setdefault("arrest_score", 0.0)
    node_state.setdefault("coherence_score", 0)
    node_state.setdefault("front_score_by_sector", {})
    node_state.setdefault("front_components", {})
    node_state.setdefault("impact_components", {})
    node_state.setdefault("arrest_components", {})
    node_state.setdefault("coherence_components", {})
    node_state.setdefault("dominant_sector", 0)
    node_state.setdefault("dominant_sector_history", [])
    node_state.setdefault("active_sectors", [])
    node_state.setdefault("front_width", 0)
    node_state.setdefault("no_progress_cycles", 0)
    node_state.setdefault("neighbor_states", {})
    node_state.setdefault("neighbor_miss_streak", {})
    node_state.setdefault("current_missing_neighbors", [])
    node_state.setdefault("new_missing_neighbors", [])
    node_state.setdefault("persistent_missing_neighbors", [])
    node_state.setdefault("recovered_neighbors", [])
    node_state.setdefault("incoming_events", [])
    node_state.setdefault("seen_event_ids", [])
    node_state.setdefault("recent_alerts", [])
    node_state.setdefault("layer1_alert", {})
    node_state.setdefault("layer2_confirmation", {})
    node_state.setdefault("last_layer1_rx", {})
    node_state.setdefault("last_layer2_rx", {})
    node_state.setdefault("prev_alert_code", 0)
    node_state.setdefault("tomo_distance_history", [])
    node_state.setdefault("last_published_layer1_signature", "")
    node_state.setdefault("last_published_layer2_signature", "")
    node_state.setdefault("last_cycle_ts", 0.0)
    node_state.setdefault("last_state_change_ts", 0.0)
    node_state.setdefault("pull_cycles", 0)
    node_state.setdefault("event_seq", 0)
    node_state.setdefault("boundary_kind", "stable")

    node_state["latency_matrix"] = []
    for i in range(number_of_nodes):
        row = []
        for j in range(number_of_nodes):
            row.append(config_json["default_latency"])
        node_state["latency_matrix"].append(row)

    state_lock = threading.Lock()

    push_queue = queue.Queue(maxsize=config_json["push_queue_maxsize"])

    push_thread = threading.Thread(target=push, args=(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue))
    push_thread.start()

    background_thread = threading.Thread(target=background, args=(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue))
    background_thread.start()

    listener_thread = threading.Thread(target=listener, args=(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue))
    listener_thread.start()

    pull_thread = threading.Thread(target=pull, args=(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue))
    pull_thread.start()

    pull_thread.join()
    listener_thread.join()
    background_thread.join()
    push_thread.join()


if __name__ == "__main__":
    main()
