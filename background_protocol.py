# EGESS - Experimental Gear for Evaluation of Swarm Systems
# Copyright (C) 2026  Nick Ivanov and ACSUS Lab <ivanov@rowan.edu>

import os
import egess_api


def _demo_mode() -> bool:
    return os.environ.get("DEMO_MODE", "0") == "1"


def _reading_for_sensor_state(sensor_state):
    state = str(sensor_state).strip().upper()
    if state == "ALERT":
        return "RED"
    if state == "RECOVERING":
        return "GREEN"
    return "BLUE"


def background_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    state_lock.acquire()
    try:
        node_state["background_hits"] = int(node_state.get("background_hits", 0)) + 1

        sensor_state = str(node_state.get("sensor_state", "NORMAL")).strip().upper()
        if sensor_state not in ("NORMAL", "ALERT", "RECOVERING"):
            sensor_state = "NORMAL"
        node_state["sensor_state"] = sensor_state
        node_state["local_reading"] = _reading_for_sensor_state(sensor_state)

        if not _demo_mode():
            egess_api.log_current_node_state(this_port, node_state)
            egess_api.write_state_change_data_point(this_port, node_state, "background_hits")
    finally:
        state_lock.release()
