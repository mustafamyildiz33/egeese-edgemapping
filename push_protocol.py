# EGESS - Experimental Gear for Evaluation of Swarm Systems
# Copyright (C) 2026  Nick Ivanov and ACSUS Lab <ivanov@rowan.edu>

import copy
import random
import egess_api


NEIGHBOR_SCOPED_TYPES = ("front_alert", "stall_notice", "recovery_notice", "alert", "heartbeat")


def push_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue, msg):
    targets = []
    md = msg.get("metadata", {})
    data = msg.get("data", {})
    msg_type = ""
    if isinstance(data, dict):
        msg_type = str(data.get("type", "")).strip().lower()

    if isinstance(md, dict) and isinstance(md.get("targets"), list) and len(md.get("targets")) > 0:
        targets = [int(x) for x in md.get("targets") if isinstance(x, int)]
    else:
        neighbors = node_state.get("neighbors", [])
        if not isinstance(neighbors, list):
            neighbors = []
        neighbors = [int(x) for x in neighbors if isinstance(x, int) and int(x) != int(this_port)]

        if msg_type in NEIGHBOR_SCOPED_TYPES and len(neighbors) > 0:
            k = int(config_json.get("alert_fanout", config_json.get("push_neighbor_fanout", 2)))
            if msg_type in ("stall_notice", "recovery_notice"):
                k = int(config_json.get("notice_fanout", k))
            k = max(1, min(int(k), len(neighbors)))
            targets = random.sample(neighbors, k)
        else:
            all_nodes = list(range(config_json["base_port"], config_json["base_port"] + number_of_nodes, 1))
            other_nodes = copy.copy(all_nodes)
            if this_port in other_nodes:
                other_nodes.remove(this_port)
            k = int(config_json.get("push_neighbor_fanout", 2))
            k = max(1, min(int(k), len(other_nodes)))
            if k > 0:
                targets = random.sample(other_nodes, k)

    for target_port in targets:
        egess_api.send_msg(config_json, node_state, state_lock, this_port, msg, target_port)
