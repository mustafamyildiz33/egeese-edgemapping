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

import json
import os
import time
import requests

RECENT_MSG_MAX = 60


def _demo_mode() -> bool:
    return os.environ.get("DEMO_MODE", "0") == "1"


def _log_enabled() -> bool:
    # In demo mode, default is OFF unless explicitly enabled.
    if _demo_mode():
        return os.environ.get("EGESS_LOG", "0") == "1"
    return True


def _data_path() -> str:
    base = os.environ.get("EGESS_LOG_DIR", ".")
    try:
        os.makedirs(base, exist_ok=True)
    except Exception:
        pass
    return os.path.join(base, "data.csv")


def log_new_node_state(this_port, apriori_node_state, aposteriori_node_state):
    if not _log_enabled():
        return
    print(
        "NODE STATE CHANGED (NODE {}):\nAPRIORI: {}\nAPOSTERIORI: {}\n".format(
            this_port, json.dumps(apriori_node_state), json.dumps(aposteriori_node_state)
        )
    )


def log_current_node_state(this_port, node_state):
    if not _log_enabled():
        return
    print(
        "NODE STATE (NODE {}):\nSTATE: {}\n".format(
            this_port, json.dumps(node_state)
        )
    )


def write_data_point(this_port, logtype, message):
    if not _log_enabled():
        return
    data_file = _data_path()
    with open(data_file, "a") as f:
        f.write("{};{};{};{}\n".format(this_port, time.time(), logtype, message))


def write_state_change_data_point(this_port, node_state, state_key):
    if not _log_enabled():
        return
    write_data_point(this_port, "state_change", "{}={}".format(state_key, node_state.get(state_key)))


def _ensure_msg_counters(node_state):
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
    return counters


def _append_recent_msg(node_state, message):
    events = node_state.get("recent_msgs", [])
    if not isinstance(events, list):
        events = []
    stamp = time.strftime("%H:%M:%S")
    events.append("[{}] {}".format(stamp, str(message)))
    if len(events) > RECENT_MSG_MAX:
        events = events[-RECENT_MSG_MAX:]
    node_state["recent_msgs"] = events


def send_msg(config_json, node_state, state_lock, this_port, msg, target_port):
    i = this_port - config_json["base_port"]
    j = target_port - config_json["base_port"]
    op = str(msg.get("op", "unknown"))

    with state_lock:
        counters = _ensure_msg_counters(node_state)
        if op == "pull":
            counters["pull_tx"] = int(counters.get("pull_tx", 0)) + 1
        elif op == "push":
            counters["push_tx"] = int(counters.get("push_tx", 0)) + 1
        _append_recent_msg(node_state, "tx:{} -> {}".format(op, target_port))

    try:
        time.sleep(node_state["latency_matrix"][i][j])
    except Exception:
        time.sleep(config_json.get("default_latency", 0.0))

    try:
        host_url = "http://" + config_json["base_host"]
        resp = requests.post("{}:{}/".format(host_url, target_port), json=msg, timeout=0.75)

        if resp.status_code == 200:
            resp_json = None
            try:
                resp_json = resp.json()
            except Exception:
                resp_json = {
                    "op": "receipt",
                    "data": {
                        "success": False,
                        "message": "invalid_json_response"
                    },
                    "metadata": {}
                }

            if _log_enabled():
                print(
                    "send_msg: SENT ({} -> {}): {}; RESPONSE: {}\n".format(
                        this_port, target_port, msg, resp_json
                    )
                )
            with state_lock:
                counters = _ensure_msg_counters(node_state)
                counters["tx_ok"] = int(counters.get("tx_ok", 0)) + 1
                _append_recent_msg(node_state, "tx_ok:{} -> {}".format(op, target_port))
            return resp_json
        else:
            if _log_enabled():
                print("ERROR: send_msg: return code is not 200.\n")
            with state_lock:
                counters = _ensure_msg_counters(node_state)
                counters["tx_fail"] = int(counters.get("tx_fail", 0)) + 1
                _append_recent_msg(node_state, "tx_fail:{} -> {} status={}".format(op, target_port, resp.status_code))
            return {
                "op": "receipt",
                "data": {
                    "success": False,
                    "message": "http_status_{}".format(resp.status_code)
                },
                "metadata": {}
            }

    except requests.exceptions.ConnectionError:
        if _log_enabled():
            print("ERROR: send_msg: Connection error.\n")
        with state_lock:
            counters = _ensure_msg_counters(node_state)
            counters["tx_conn_error"] = int(counters.get("tx_conn_error", 0)) + 1
            counters["tx_fail"] = int(counters.get("tx_fail", 0)) + 1
            _append_recent_msg(node_state, "tx_conn_error:{} -> {}".format(op, target_port))
        return {
            "op": "receipt",
            "data": {
                "success": False,
                "message": "connection_error"
            },
            "metadata": {}
        }
    except requests.exceptions.Timeout:
        if _log_enabled():
            print("ERROR: send_msg: Timeout.\n")
        with state_lock:
            counters = _ensure_msg_counters(node_state)
            counters["tx_timeout"] = int(counters.get("tx_timeout", 0)) + 1
            counters["tx_fail"] = int(counters.get("tx_fail", 0)) + 1
            _append_recent_msg(node_state, "tx_timeout:{} -> {}".format(op, target_port))
        return {
            "op": "receipt",
            "data": {
                "success": False,
                "message": "timeout"
            },
            "metadata": {}
        }
    except Exception:
        if _log_enabled():
            print("ERROR: send_msg: Unknown error.\n")
        with state_lock:
            counters = _ensure_msg_counters(node_state)
            counters["tx_fail"] = int(counters.get("tx_fail", 0)) + 1
            _append_recent_msg(node_state, "tx_unknown_error:{} -> {}".format(op, target_port))
        return {
            "op": "receipt",
            "data": {
                "success": False,
                "message": "unknown_error"
            },
            "metadata": {}
        }
