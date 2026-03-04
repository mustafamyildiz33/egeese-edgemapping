
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

    app.run(host=config_json["base_host"], port=this_port)


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

    with open(node_state_init_file) as file:
        node_state = json.load(file)

    grid_size = int(node_state.get("grid_size", 8))
    base_port = int(config_json.get("base_port", 9000))

    idx = this_port - base_port
    gx = int(idx % grid_size)
    gy = int(idx // grid_size)

    node_state["grid_size"] = grid_size
    node_state["grid_pos"] = [gx, gy]

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
