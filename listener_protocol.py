# EGESS - Experimental Gear for Evaluation of Swarm Systems
# Copyright (C) 2026  Nick Ivanov and ACSUS Lab <ivanov@rowan.edu>

import copy
import os
import queue
import time
import egess_api


EVENT_TYPES = ("front_alert", "stall_notice", "recovery_notice")


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


def _reading_for_sensor_state(sensor_state):
    state = str(sensor_state).strip().upper()
    if state == "ALERT":
        return "RED"
    if state == "RECOVERING":
        return "GREEN"
    return "BLUE"


def _reset_runtime_state(node_state):
    node_state["sensor_state"] = "NORMAL"
    node_state["local_reading"] = "BLUE"
    node_state["protocol_state"] = "NORMAL"
    node_state["boundary_kind"] = "stable"
    node_state["dfa_state"] = 1

    node_state["score"] = 0.0
    node_state["raw_score"] = 0.0
    node_state["score_delta"] = 0.0
    node_state["score_trend"] = "steady (0)"
    node_state["score_bucket"] = 0

    node_state["front_score"] = 0.0
    node_state["impact_score"] = 0.0
    node_state["arrest_score"] = 0.0
    node_state["coherence_score"] = 0
    node_state["front_score_by_sector"] = {}
    node_state["front_components"] = {}
    node_state["impact_components"] = {}
    node_state["arrest_components"] = {}
    node_state["coherence_components"] = {}

    node_state["dominant_sector"] = 0
    node_state["dominant_sector_history"] = []
    node_state["active_sectors"] = []
    node_state["front_width"] = 0
    node_state["no_progress_cycles"] = 0

    node_state["neighbor_states"] = {}
    node_state["neighbor_miss_streak"] = {}
    node_state["current_missing_neighbors"] = []
    node_state["new_missing_neighbors"] = []
    node_state["persistent_missing_neighbors"] = []
    node_state["recovered_neighbors"] = []

    node_state["incoming_events"] = []
    node_state["seen_event_ids"] = []
    node_state["recent_alerts"] = []

    node_state["last_cycle_ts"] = 0.0
    node_state["last_state_change_ts"] = 0.0
    node_state["pull_cycles"] = 0
    node_state["event_seq"] = 0

    counters = node_state.get("msg_counters", {})
    if not isinstance(counters, dict):
        counters = {}
    for key in (
        "pull_rx",
        "push_rx",
        "pull_tx",
        "push_tx",
        "tx_ok",
        "tx_fail",
        "tx_timeout",
        "tx_conn_error",
    ):
        counters[key] = 0
    node_state["msg_counters"] = counters
    node_state["recent_msgs"] = []


def _remember_event(node_state, mtype, event_id, origin, relay, payload):
    seen = node_state.get("seen_event_ids", [])
    if not isinstance(seen, list):
        seen = []
    seen_set = set(seen)
    if event_id in seen_set:
        node_state["seen_event_ids"] = seen
        return False

    if isinstance(event_id, str) and len(event_id) > 0:
        seen.append(event_id)
        if len(seen) > 200:
            seen = seen[-200:]
        node_state["seen_event_ids"] = seen

    incoming = node_state.get("incoming_events", [])
    if not isinstance(incoming, list):
        incoming = []
    incoming.append(
        {
            "type": str(mtype),
            "event_id": str(event_id),
            "origin": int(origin) if isinstance(origin, int) else 0,
            "relay": int(relay) if isinstance(relay, int) else 0,
            "ts": float(time.time()),
            "state": str(payload.get("state", "")) if isinstance(payload, dict) else "",
        }
    )
    if len(incoming) > 120:
        incoming = incoming[-120:]
    node_state["incoming_events"] = incoming

    recent = node_state.get("recent_alerts", [])
    if not isinstance(recent, list):
        recent = []
    label = str(mtype)
    if isinstance(payload, dict):
        state = str(payload.get("state", "")).strip().upper()
        if state:
            label = "{}:{}".format(label, state)
    recent.append(label)
    if len(recent) > 20:
        recent = recent[-20:]
    node_state["recent_alerts"] = recent
    return True


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

        with state_lock:
            faults = _faults(node_state)
            faults["period_sec"] = period_sec
            if fault == "reset":
                faults["crash_sim"] = False
                faults["lie_sensor"] = False
                faults["flap"] = False
                _reset_runtime_state(node_state)
                _add_recent_msg(node_state, "runtime_reset")
            elif fault in ("crash_sim", "lie_sensor", "flap"):
                faults[fault] = enable
            else:
                return {
                    "op": "receipt",
                    "data": {"success": False, "message": "unknown_fault"},
                    "metadata": {}
                }

        if _verbose_logs() and fault == "crash_sim":
            egess_api.write_data_point(this_port, "state_change", "DESTROYED={}".format(bool(enable)))

        return {
            "op": "receipt",
            "data": {"success": True, "message": "fault_updated", "faults": faults},
            "metadata": {}
        }

    if op == "inject_state":
        data = msg.get("data", {})
        if not isinstance(data, dict):
            data = {}
        sensor_state = str(data.get("sensor_state", "NORMAL")).strip().upper()
        if sensor_state not in ("NORMAL", "ALERT", "RECOVERING"):
            return {
                "op": "receipt",
                "data": {"success": False, "message": "unknown_sensor_state"},
                "metadata": {}
            }

        with state_lock:
            node_state["sensor_state"] = sensor_state
            node_state["local_reading"] = _reading_for_sensor_state(sensor_state)
            _add_recent_msg(node_state, "sensor_state={}".format(sensor_state))

        return {
            "op": "receipt",
            "data": {
                "success": True,
                "message": "sensor_state_updated",
                "sensor_state": sensor_state,
                "local_reading": _reading_for_sensor_state(sensor_state)
            },
            "metadata": {}
        }

    with state_lock:
        crash_sim = bool(_faults(node_state).get("crash_sim", False))

    if crash_sim:
        with state_lock:
            _add_recent_msg(node_state, "drop:{} (crash_sim)".format(op))
        time.sleep(1.1)
        return {
            "op": "receipt",
            "data": {"success": False, "message": "node_unavailable(crash_sim)"},
            "metadata": {}
        }

    if op == "pull":
        with state_lock:
            counters, _ = _touch_msg_telemetry(node_state)
            counters["pull_rx"] = int(counters.get("pull_rx", 0)) + 1
            origin = msg.get("metadata", {}).get("origin", "viz")
            _add_recent_msg(node_state, "rx:pull <- {}".format(origin))
            snapshot = copy.deepcopy(node_state)

        if _verbose_logs():
            print("PULL REQUEST RECEIVED\n")
            egess_api.write_data_point(this_port, "pull_request_received", str(origin))
        return {
            "op": "receipt",
            "data": {"success": True, "message": "", "node_state": snapshot},
            "metadata": {}
        }

    if op != "push":
        if _verbose_logs():
            print("ERROR: listener_protocol: unknown type of message: {}\n".format(op))
        return {
            "op": "receipt",
            "data": {"success": False, "message": "unknown operation"},
            "metadata": {}
        }

    metadata = msg.get("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
        msg["metadata"] = metadata

    try:
        forward_count = int(metadata.get("forward_count", 0))
    except Exception:
        forward_count = 0

    max_forwards = int(config_json.get("max_forwards", 8))
    if forward_count >= max_forwards:
        return {
            "op": "receipt",
            "data": {"success": False, "message": "message is not enqueued"},
            "metadata": {}
        }

    data = msg.get("data", {})
    if not isinstance(data, dict):
        data = {}
    mtype = str(data.get("type", "")).strip()
    origin = msg.get("metadata", {}).get("origin", 0)
    relay = msg.get("metadata", {}).get("relay", 0)
    event_id = str(data.get("event_id", "")).strip()

    with state_lock:
        counters, _ = _touch_msg_telemetry(node_state)
        counters["push_rx"] = int(counters.get("push_rx", 0)) + 1
        _add_recent_msg(node_state, "rx:push <- {}".format(origin))
        node_state["accepted_messages"] = int(node_state.get("accepted_messages", 0)) + 1

        if isinstance(relay, int) and relay != 0:
            known_nodes = node_state.get("known_nodes", [])
            if not isinstance(known_nodes, list):
                known_nodes = []
            if relay not in known_nodes and relay != this_port:
                known_nodes.append(relay)
            node_state["known_nodes"] = known_nodes

        duplicate = False
        if mtype in EVENT_TYPES and isinstance(event_id, str) and len(event_id) > 0:
            duplicate = not _remember_event(node_state, mtype, event_id, origin, relay, data)
            if duplicate:
                _add_recent_msg(node_state, "dup:{} {}".format(mtype, event_id))

        if not _demo_mode():
            egess_api.write_state_change_data_point(this_port, node_state, "accepted_messages")
            egess_api.write_state_change_data_point(this_port, node_state, "known_nodes")

    if duplicate:
        return {
            "op": "receipt",
            "data": {"success": True, "message": "duplicate_ignored"},
            "metadata": {}
        }

    fwd_msg = copy.deepcopy(msg)
    fwd_msg["metadata"]["relay"] = this_port
    fwd_msg["metadata"]["forward_count"] = forward_count + 1
    try:
        push_queue.put_nowait(fwd_msg)
    except queue.Full:
        with state_lock:
            _add_recent_msg(node_state, "drop:push queue_full")
        return {
            "op": "receipt",
            "data": {"success": False, "message": "queue_full"},
            "metadata": {}
        }

    with state_lock:
        _add_recent_msg(node_state, "enqueue:push fwd={}".format(fwd_msg["metadata"]["forward_count"]))

    return {
        "op": "receipt",
        "data": {"success": True, "message": "message enqueued"},
        "metadata": {}
    }
