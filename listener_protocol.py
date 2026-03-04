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

import os
import time
import egess_api


def _demo_mode() -> bool:
    return os.environ.get("DEMO_MODE", "0") == "1"


def _verbose_logs() -> bool:
    if not _demo_mode():
        return True
    return os.environ.get("EGESS_LOG", "0") == "1"


def _faults(node_state):
    faults = node_state.get("faults", {})
    if not isinstance(faults, dict):
        faults = {}
        node_state["faults"] = faults
    faults["crash_sim"] = bool(faults.get("crash_sim", False))
    faults["lie_sensor"] = bool(faults.get("lie_sensor", False))
    faults["flap"] = bool(faults.get("flap", False))
    faults["period_sec"] = int(faults.get("period_sec", 4))
    return faults


def _touch_msg_telemetry(node_state):
    counters = node_state.get("msg_counters", {})
    if not isinstance(counters, dict):
        counters = {}
    defaults = {
        "pull_rx": 0,
        "push_rx": 0,
        "pull_tx": 0,
        "push_tx": 0,
        "tx_ok": 0,
        "tx_fail": 0,
        "tx_timeout": 0,
        "tx_conn_error": 0,
    }
    for k, v in defaults.items():
        try:
            counters[k] = int(counters.get(k, v))
        except Exception:
            counters[k] = int(v)
    node_state["msg_counters"] = counters

    events = node_state.get("recent_msgs", [])
    if not isinstance(events, list):
        events = []
    node_state["recent_msgs"] = events
    return counters, events


def _add_recent_msg(node_state, message):
    _, events = _touch_msg_telemetry(node_state)
    events.append("[{}] {}".format(time.strftime("%H:%M:%S"), str(message)))
    if len(events) > 60:
        del events[:-60]


def listener_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue, msg):
    op = msg.get("op", "")

    if op == "inject_fault":
        data = msg.get("data", {})
        if not isinstance(data, dict):
            data = {}
        fault = str(data.get("fault", "")).strip()
        enable = bool(data.get("enable", True))
        period_sec = int(data.get("period_sec", 4))
        if period_sec < 1:
            period_sec = 1

        state_lock.acquire()
        try:
            faults = _faults(node_state)
            faults["period_sec"] = period_sec
            if fault == "reset":
                faults["crash_sim"] = False
                faults["lie_sensor"] = False
                faults["flap"] = False
            elif fault in ("crash_sim", "lie_sensor", "flap"):
                faults[fault] = enable
            else:
                return {
                    "op": "receipt",
                    "data": {
                        "success": False,
                        "message": "unknown_fault"
                    },
                    "metadata": {}
                }
        finally:
            state_lock.release()

        return {
            "op": "receipt",
            "data": {
                "success": True,
                "message": "fault_updated",
                "faults": faults
            },
            "metadata": {}
        }

    state_lock.acquire()
    try:
        crash_sim = bool(_faults(node_state).get("crash_sim", False))
    finally:
        state_lock.release()

    # Simulate a node that exists but is not responsive enough for pull timeouts.
    if crash_sim:
        state_lock.acquire()
        try:
            _add_recent_msg(node_state, "drop:{} (crash_sim)".format(op))
        finally:
            state_lock.release()
        time.sleep(1.1)
        return {
            "op": "receipt",
            "data": {
                "success": False,
                "message": "node_unavailable(crash_sim)"
            },
            "metadata": {}
        }

    if op == "pull":
        state_lock.acquire()
        try:
            counters, _ = _touch_msg_telemetry(node_state)
            counters["pull_rx"] = int(counters.get("pull_rx", 0)) + 1
            origin = msg.get("metadata", {}).get("origin", "viz")
            _add_recent_msg(node_state, "rx:pull <- {}".format(origin))
        finally:
            state_lock.release()

        if _verbose_logs():
            print("PULL REQUEST RECEIVED\n")
            egess_api.write_data_point(this_port, "pull_request_received", str(0))
        return {
            "op": "receipt",
            "data": {
                "success": True,
                "message": "",
                "node_state": node_state
            },
            "metadata": {}
        }

    elif op == "push":
        metadata = msg.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
            msg["metadata"] = metadata

        try:
            forward_count = int(metadata.get("forward_count", 0))
        except Exception:
            forward_count = 0

        max_forwards = int(config_json.get("max_forwards", 8))

        if forward_count < max_forwards:
            state_lock.acquire()
            try:
                counters, _ = _touch_msg_telemetry(node_state)
                counters["push_rx"] = int(counters.get("push_rx", 0)) + 1
                origin = msg.get("metadata", {}).get("origin", 0)
                _add_recent_msg(node_state, "rx:push <- {}".format(origin))
                node_state["accepted_messages"] = int(node_state.get("accepted_messages", 0)) + 1

                relay = msg.get("metadata", {}).get("relay", 0)
                if isinstance(relay, int) and relay != 0:
                    if "known_nodes" not in node_state or not isinstance(node_state["known_nodes"], list):
                        node_state["known_nodes"] = []
                    if relay not in node_state["known_nodes"] and relay != this_port:
                        node_state["known_nodes"].append(relay)

                data = msg.get("data", {})
                mtype = ""
                if isinstance(data, dict):
                    mtype = data.get("type", "")

                origin = msg.get("metadata", {}).get("origin", 0)
                now = time.time()

                if mtype == "heartbeat":
                    if isinstance(origin, int) and origin != 0:
                        if "last_seen" not in node_state or not isinstance(node_state["last_seen"], dict):
                            node_state["last_seen"] = {}
                        node_state["last_seen"][str(origin)] = now

                if mtype == "missing_report":
                    missing_port = 0
                    if isinstance(data, dict):
                        missing_port = data.get("missing_port", 0)
                    if isinstance(origin, int) and origin != 0 and isinstance(missing_port, int) and missing_port != 0:
                        if "missing_reports" not in node_state or not isinstance(node_state["missing_reports"], dict):
                            node_state["missing_reports"] = {}
                        key = str(missing_port)
                        node_state["missing_reports"][key] = int(node_state["missing_reports"].get(key, 0)) + 1

                if not _demo_mode():
                    egess_api.write_state_change_data_point(this_port, node_state, "accepted_messages")
                    egess_api.write_state_change_data_point(this_port, node_state, "known_nodes")

            finally:
                state_lock.release()

            msg["metadata"]["relay"] = this_port
            msg["metadata"]["forward_count"] = forward_count + 1
            push_queue.put(msg)
            state_lock.acquire()
            try:
                _add_recent_msg(node_state, "enqueue:push fwd={}".format(msg["metadata"]["forward_count"]))
            finally:
                state_lock.release()

            return {
                "op": "receipt",
                "data": {
                    "success": True,
                    "message": "message enqueued"
                },
                "metadata": {}
            }
        else:
            return {
                "op": "receipt",
                "data": {
                    "success": False,
                    "message": "message is not enqueued"
                },
                "metadata": {}
            }

    else:
        if _verbose_logs():
            print("ERROR: listener_protocol: unknown type of message: {}\n".format(op))
        return {
            "op": "receipt",
            "data": {
                "success": False,
                "message": "unknown operation"
            },
            "metadata": {}
        }
