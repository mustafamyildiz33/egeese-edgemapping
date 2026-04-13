# EGESS - Experimental Gear for Evaluation of Swarm Systems
# Copyright (C) 2026  Nick Ivanov and ACSUS Lab <ivanov@rowan.edu>

import math
import queue
import time
import egess_api


STATE_MISSING = 0
STATE_STABLE = 1
STATE_VERIFY = 2
STATE_BOUNDARY = 3

SECTOR_COUNT = 6
ACTIVE_STATES = ("WATCH", "WARNING", "IMPACT", "STALLED", "CONTAINED", "RECOVERING")
ALERT_BITS = {0: "00", 1: "01", 2: "10", 3: "11"}
ALERT_LEVELS = {0: "NORMAL", 1: "WATCH", 2: "WARNING", 3: "IMPACT"}
DIR_LABELS = {0: "", 1: "E", 2: "NE", 3: "NW", 4: "W", 5: "SW", 6: "SE"}


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


def _score_bucket(score, t_low, t_high):
    value = float(score)
    if value <= float(t_low):
        return 0
    if value < float(t_high):
        return 1
    if value < float(t_high) + 2.0:
        return 2
    return 3


def _score_trend(delta):
    if delta > 0:
        return "increasing (+{:.1f})".format(delta)
    if delta < 0:
        return "decreasing ({:.1f})".format(delta)
    return "steady (0)"


def _empty_sector_map():
    return {str(i): 0.0 for i in range(1, SECTOR_COUNT + 1)}


def _empty_tomography():
    return {
        "absence_vector": [0, 0, 0, 0, 0, 0],
        "t_gradient": [0, 0, 0, 0, 0, 0],
        "delta_bits": [0, 0, 0, 0, 0, 0],
        "direction_sector": 0,
        "direction_label": "",
        "angle_deg": 0.0,
        "sin_sum": 0.0,
        "cos_sum": 0.0,
        "distance_hops": 99.0,
        "speed_hops_per_cycle": 0.0,
        "eta_cycles": 99.0,
        "phase": "CLEAR",
        "strength": 0.0,
        "message": "No directional confirmation",
    }


def _alert_code(protocol_state, score):
    state = str(protocol_state).strip().upper()
    if state == "IMPACT":
        return 3
    if state == "WARNING":
        return 2
    if state in ("WATCH", "STALLED", "CONTAINED", "RECOVERING"):
        return 1 if float(score) > 0.0 or state != "NORMAL" else 0
    return 0


def _alert_bits(code):
    return str(ALERT_BITS.get(int(code), "00"))


def _alert_level(code):
    return str(ALERT_LEVELS.get(int(code), "NORMAL"))


def _alert_delta_bits(current_code, previous_code):
    current_value = int(current_code)
    diff = int(current_code) - int(previous_code)
    if current_value <= 0 and diff <= 0:
        return 0
    if diff >= 2:
        return 3
    if diff == 1:
        return 2
    if current_value > 0:
        return 1
    return 0


def _port_to_rc(base_port, port, grid):
    idx = int(port) - int(base_port)
    return int(idx // grid), int(idx % grid)


def _hex_center_xy(row, col):
    x = math.sqrt(3.0) * (float(col) + (0.5 if int(row) % 2 == 1 else 0.0))
    y = 1.5 * float(row)
    return x, y


def _sector_for_port(config_json, node_state, this_port, target_port):
    try:
        target = int(target_port)
    except Exception:
        return 0

    base_port = int(config_json.get("base_port", 9000))
    grid = int(node_state.get("grid_size", config_json.get("grid_size", 8)))
    if target == int(this_port) or target < base_port:
        return 0

    try:
        r0, c0 = _port_to_rc(base_port, int(this_port), grid)
        r1, c1 = _port_to_rc(base_port, target, grid)
    except Exception:
        return 0

    x0, y0 = _hex_center_xy(r0, c0)
    x1, y1 = _hex_center_xy(r1, c1)
    dx = float(x1) - float(x0)
    dy = float(y1) - float(y0)
    if abs(dx) < 1e-9 and abs(dy) < 1e-9:
        return 0

    angle_deg = (math.degrees(math.atan2(dy, dx)) + 360.0) % 360.0
    return int(((angle_deg + 30.0) % 360.0) // 60.0) + 1


def _adjacent_sectors(sector):
    if int(sector) < 1 or int(sector) > SECTOR_COUNT:
        return set()
    left = int(sector) - 1 if int(sector) > 1 else SECTOR_COUNT
    right = int(sector) + 1 if int(sector) < SECTOR_COUNT else 1
    return {left, right}


def _has_adjacent_sectors(sectors):
    sector_set = set(int(x) for x in sectors if 1 <= int(x) <= SECTOR_COUNT)
    for sector in list(sector_set):
        if len(_adjacent_sectors(sector).intersection(sector_set)) > 0:
            return True
    return False


def _dominant_sector(front_scores, previous_sector):
    best_sector = 0
    best_value = 0.0
    for sector in range(1, SECTOR_COUNT + 1):
        value = float(front_scores.get(str(sector), 0.0))
        if value > best_value:
            best_sector = sector
            best_value = value
    if best_value <= 0.0:
        return 0

    if 1 <= int(previous_sector) <= SECTOR_COUNT:
        prev_value = float(front_scores.get(str(int(previous_sector)), 0.0))
        if abs(prev_value - best_value) < 1e-9:
            return int(previous_sector)
    return int(best_sector)


def _pull_neighbor_snapshot(config_json, node_state, state_lock, this_port, nport):
    msg = {
        "op": "pull",
        "data": {"kind": "neighbor_status"},
        "metadata": {"origin": this_port}
    }
    resp = egess_api.send_msg(config_json, node_state, state_lock, this_port, msg, int(nport))
    if not resp or resp.get("op") != "receipt":
        return False, None
    data = resp.get("data", {})
    if not bool(data.get("success", False)):
        return False, None
    nstate = data.get("node_state", {})
    if not isinstance(nstate, dict):
        nstate = {}
    return True, nstate


def _probe_neighbors(config_json, node_state, state_lock, this_port, neighbors):
    snapshots = {}
    missing_neighbors = set()
    for nport in neighbors:
        ok, nstate = _pull_neighbor_snapshot(config_json, node_state, state_lock, this_port, int(nport))
        if not ok:
            missing_neighbors.add(int(nport))
            continue
        snapshots[int(nport)] = nstate
    return snapshots, missing_neighbors


def _classify_neighbors(node_state, neighbors, missing_neighbors):
    prev_states = node_state.get("neighbor_states", {})
    if not isinstance(prev_states, dict):
        prev_states = {}
    prev_streak = node_state.get("neighbor_miss_streak", {})
    if not isinstance(prev_streak, dict):
        prev_streak = {}

    next_states = {}
    next_streak = {}
    new_missing = []
    persistent_missing = []
    recovered = []
    current_missing = []

    for neighbor in neighbors:
        key = str(int(neighbor))
        prev_state = str(prev_states.get(key, "alive")).lower()
        prev_misses = _to_int(prev_streak.get(key, 0), 0)

        if int(neighbor) in missing_neighbors:
            misses = int(prev_misses) + 1
            next_streak[key] = misses
            if misses >= 2:
                next_states[key] = "missing"
                current_missing.append(int(neighbor))
                if prev_state == "missing":
                    persistent_missing.append(int(neighbor))
                else:
                    new_missing.append(int(neighbor))
            else:
                next_states[key] = "suspect"
        else:
            next_streak[key] = 0
            if prev_state in ("suspect", "missing"):
                next_states[key] = "recovered"
                recovered.append(int(neighbor))
            else:
                next_states[key] = "alive"

    return {
        "next_states": next_states,
        "next_streak": next_streak,
        "new_missing": sorted(new_missing),
        "persistent_missing": sorted(persistent_missing),
        "recovered": sorted(recovered),
        "current_missing": sorted(current_missing),
    }


def _count_by_sector(config_json, node_state, this_port, ports):
    counts = _empty_sector_map()
    for port in ports:
        sector = _sector_for_port(config_json, node_state, this_port, int(port))
        if 1 <= int(sector) <= SECTOR_COUNT:
            key = str(int(sector))
            counts[key] = float(counts.get(key, 0.0)) + 1.0
    return counts


def _neighbor_alert_snapshot(snapshot):
    if not isinstance(snapshot, dict):
        return 0, 0

    layer1 = snapshot.get("layer1_alert", {})
    if isinstance(layer1, dict) and len(layer1) > 0:
        code = _to_int(layer1.get("alert_code", 0), 0)
        delta_bits = _to_int(layer1.get("delta_bits", 0), 0)
        return max(0, min(code, 3)), max(0, min(delta_bits, 3))

    code = _alert_code(snapshot.get("protocol_state", "NORMAL"), snapshot.get("score", 0.0))
    score_delta = _to_float(snapshot.get("score_delta", 0.0), 0.0)
    if code <= 0:
        delta_bits = 0
    elif score_delta >= 2.0:
        delta_bits = 3
    elif score_delta > 0.0:
        delta_bits = 2
    else:
        delta_bits = 1
    return max(0, min(code, 3)), max(0, min(delta_bits, 3))


def _compute_tomography(config_json, node_state, this_port, neighbors, snapshots, current_missing):
    result = _empty_tomography()
    absence_vector = [0, 0, 0, 0, 0, 0]
    t_gradient = [0, 0, 0, 0, 0, 0]
    delta_bits = [0, 0, 0, 0, 0, 0]

    for neighbor in neighbors:
        sector = _sector_for_port(config_json, node_state, this_port, int(neighbor))
        if sector < 1 or sector > SECTOR_COUNT:
            continue
        idx = int(sector) - 1
        if int(neighbor) in current_missing:
            absence_vector[idx] = 1
            t_gradient[idx] = 3
            delta_bits[idx] = 3
            continue

        code, delta = _neighbor_alert_snapshot(snapshots.get(int(neighbor), {}))
        t_gradient[idx] = max(int(t_gradient[idx]), int(code))
        delta_bits[idx] = max(int(delta_bits[idx]), int(delta))

    sin_sum = 0.0
    cos_sum = 0.0
    total_weight = 0.0
    for idx in range(SECTOR_COUNT):
        angle = math.radians(float(idx) * 60.0)
        weight = 5.0 * float(absence_vector[idx]) + float(t_gradient[idx]) + 0.5 * float(delta_bits[idx])
        sin_sum += weight * math.sin(angle)
        cos_sum += weight * math.cos(angle)
        total_weight += weight

    direction_sector = 0
    direction_label = ""
    angle_deg = 0.0
    if total_weight > 0.0:
        angle_deg = (math.degrees(math.atan2(sin_sum, cos_sum)) + 360.0) % 360.0
        direction_sector = int(round(angle_deg / 60.0)) % 6 + 1
        direction_label = DIR_LABELS.get(int(direction_sector), "")

    max_gradient = max([int(x) for x in t_gradient] + [0])
    distance_hops = 99.0
    if max_gradient > 0:
        distance_hops = round(12.0 / float(max_gradient), 1)

    prev_history = node_state.get("tomo_distance_history", [])
    if not isinstance(prev_history, list):
        prev_history = []
    dist_history = list(prev_history[-3:])
    dist_history.append(float(distance_hops))

    valid_history = [float(x) for x in dist_history if float(x) < 90.0]
    speed_hops_per_cycle = 0.0
    if len(valid_history) >= 2:
        speed_hops_per_cycle = round((float(valid_history[0]) - float(valid_history[-1])) / float(len(valid_history) - 1), 1)

    eta_cycles = 99.0
    if speed_hops_per_cycle > 0.0 and float(distance_hops) < 90.0:
        eta_cycles = round(float(distance_hops) / float(speed_hops_per_cycle), 1)

    prev_vector = node_state.get("layer2_confirmation", {}).get("absence_vector", [0, 0, 0, 0, 0, 0])
    if not isinstance(prev_vector, list) or len(prev_vector) != SECTOR_COUNT:
        prev_vector = [0, 0, 0, 0, 0, 0]
    growing = 0
    shrinking = 0
    for idx in range(SECTOR_COUNT):
        if int(absence_vector[idx]) > int(prev_vector[idx]):
            growing += 1
        elif int(absence_vector[idx]) < int(prev_vector[idx]):
            shrinking += 1

    rising_deltas = len([x for x in delta_bits if int(x) >= 2])
    stable_deltas = len([x for x in delta_bits if int(x) == 1])
    phase = "CLEAR"
    if total_weight > 0.0:
        if sum(int(x) for x in absence_vector) > 0 and max_gradient >= 3:
            phase = "IMPACT"
        elif growing > 0 or rising_deltas >= 2:
            phase = "APPROACHING"
        elif shrinking > 0 and growing == 0:
            phase = "RECOVERING"
        elif stable_deltas >= 2 and rising_deltas == 0 and growing == 0:
            phase = "CONTAINED"
        else:
            phase = "MONITORING"

    strength = 0.0
    strength += float(sum(int(x) for x in absence_vector)) * 2.0
    strength += float(sum(int(x) for x in t_gradient)) * 0.3
    strength += float(rising_deltas) * 0.5
    strength = round(strength, 1)

    if phase == "CLEAR":
        message = "No directional confirmation"
    else:
        message = "{} from {} | dist={} hops | speed={} hops/cycle | eta={} cycles".format(
            phase,
            direction_label or "?",
            distance_hops if distance_hops < 90.0 else "?",
            speed_hops_per_cycle if speed_hops_per_cycle > 0.0 else 0.0,
            eta_cycles if eta_cycles < 90.0 else "?",
        )

    result["absence_vector"] = absence_vector
    result["t_gradient"] = t_gradient
    result["delta_bits"] = delta_bits
    result["direction_sector"] = int(direction_sector)
    result["direction_label"] = str(direction_label)
    result["angle_deg"] = round(float(angle_deg), 1)
    result["sin_sum"] = round(float(sin_sum), 2)
    result["cos_sum"] = round(float(cos_sum), 2)
    result["distance_hops"] = round(float(distance_hops), 1)
    result["speed_hops_per_cycle"] = round(float(speed_hops_per_cycle), 1)
    result["eta_cycles"] = round(float(eta_cycles), 1)
    result["phase"] = str(phase)
    result["strength"] = float(strength)
    result["message"] = str(message)
    result["history"] = [round(float(x), 1) for x in dist_history[-4:]]
    return result


def _consume_events(config_json, node_state, this_port):
    events = node_state.get("incoming_events", [])
    if not isinstance(events, list):
        events = []

    now_ts = float(time.time())
    last_cycle_ts = _to_float(node_state.get("last_cycle_ts", 0.0), 0.0)
    fresh_window = _to_float(
        config_json.get(
            "fresh_event_window_sec",
            max(2.0, float(config_json.get("pull_period", 2)) * 2.0)
        ),
        4.5,
    )
    keep_horizon = max(30.0, fresh_window * 8.0)

    front_alert_relays = {str(i): set() for i in range(1, SECTOR_COUNT + 1)}
    confirmation_relays = {str(i): set() for i in range(1, SECTOR_COUNT + 1)}
    recovery_notice_relays = {str(i): set() for i in range(1, SECTOR_COUNT + 1)}
    stall_notices = 0
    retained = []

    for event in events:
        if not isinstance(event, dict):
            continue
        event_ts = _to_float(event.get("ts", 0.0), 0.0)
        if now_ts - event_ts <= keep_horizon:
            retained.append(event)

        if event_ts <= last_cycle_ts:
            continue
        if now_ts - event_ts > fresh_window:
            continue

        event_type = str(event.get("type", "")).strip()
        relay = event.get("relay", event.get("origin", 0))
        sector = _sector_for_port(config_json, node_state, this_port, relay)

        sector_key = str(int(sector))
        relay_key = _to_int(relay, 0)

        if event_type in ("front_alert", "alert_state") and 1 <= int(sector) <= SECTOR_COUNT:
            front_alert_relays[sector_key].add(relay_key)
        elif event_type == "confirmation_notice" and 1 <= int(sector) <= SECTOR_COUNT:
            phase = str(event.get("phase", event.get("state", ""))).strip().upper()
            if phase == "RECOVERING":
                recovery_notice_relays[sector_key].add(relay_key)
            else:
                confirmation_relays[sector_key].add(relay_key)
        elif event_type == "recovery_notice" and 1 <= int(sector) <= SECTOR_COUNT:
            recovery_notice_relays[sector_key].add(relay_key)
        elif event_type == "stall_notice":
            stall_notices += 1

    node_state["incoming_events"] = retained[-120:]
    front_alerts = {key: float(len(value)) for key, value in front_alert_relays.items()}
    confirmation_alerts = {key: float(len(value)) for key, value in confirmation_relays.items()}
    recovery_notices = {key: float(len(value)) for key, value in recovery_notice_relays.items()}
    return front_alerts, confirmation_alerts, recovery_notices, int(stall_notices)


def _front_scores(new_missing_by_sector, persistent_by_sector, alert_by_sector, confirmation_by_sector, recovery_by_sector, history):
    scores = _empty_sector_map()
    updated_history = list(history[-2:]) if isinstance(history, list) else []

    for sector in range(1, SECTOR_COUNT + 1):
        key = str(sector)
        alert_count = float(alert_by_sector.get(key, 0.0))
        confirm_count = float(confirmation_by_sector.get(key, 0.0))
        disagreement = 1.0 if alert_count > 0.0 else 0.0
        corroboration = 1.0 if alert_count > 1.0 else 0.0
        confirmation_bonus = min(1.0, 0.5 * float(confirm_count))
        raw_value = (
            2.0 * float(new_missing_by_sector.get(key, 0.0)) +
            float(disagreement) +
            float(corroboration) +
            float(confirmation_bonus) +
            0.5 * float(persistent_by_sector.get(key, 0.0)) -
            1.0 * float(recovery_by_sector.get(key, 0.0))
        )
        if raw_value > 0.0 and sector in updated_history and updated_history.count(sector) >= 2:
            raw_value += 1.0
        scores[key] = max(0.0, float(raw_value))
    return scores


def _sector_keys_with_signal(*sector_maps):
    active = set()
    for sector_map in sector_maps:
        if not isinstance(sector_map, dict):
            continue
        for key, value in sector_map.items():
            if _to_float(value, 0.0) > 0.0:
                active.add(int(key))
    return sorted(active)


def _event_id(node_state, prefix, this_port):
    seq = _to_int(node_state.get("event_seq", 0), 0) + 1
    node_state["event_seq"] = seq
    return "{}-{}-{}-{}".format(str(prefix), int(this_port), int(time.time() * 1000), int(seq))


def _queue_event(node_state, state_lock, push_queue, msg):
    try:
        push_queue.put_nowait(msg)
        return True
    except queue.Full:
        with state_lock:
            node_state["last_queue_drop"] = str(msg.get("data", {}).get("event_id", "queue_drop"))
        return False


def _state_rank(protocol_state):
    order = {
        "NORMAL": 0,
        "WATCH": 1,
        "WARNING": 2,
        "IMPACT": 3,
        "STALLED": 4,
        "CONTAINED": 5,
        "RECOVERING": 6,
    }
    return int(order.get(str(protocol_state).strip().upper(), 0))


def _publish_notice(config_json, node_state, state_lock, this_port, push_queue, notice_type, protocol_state, score, dominant_sector):
    msg = {
        "op": "push",
        "data": {
            "type": str(notice_type),
            "event_id": _event_id(node_state, notice_type, this_port),
            "state": str(protocol_state),
            "score": float(score),
            "dominant_sector": int(dominant_sector),
            "ts": float(time.time()),
        },
        "metadata": {
            "origin": int(this_port),
            "relay": int(this_port),
            "forward_count": 0,
        }
    }
    _queue_event(node_state, state_lock, push_queue, msg)


def _publish_local_layer_message(node_state, state_lock, this_port, push_queue, neighbors, notice_type, payload):
    msg = {
        "op": "push",
        "data": dict(payload),
        "metadata": {
            "origin": int(this_port),
            "relay": int(this_port),
            "forward_count": 0,
            "targets": [int(x) for x in neighbors if isinstance(x, int) and int(x) != int(this_port)],
            "no_forward": True,
        },
    }
    msg["data"]["type"] = str(notice_type)
    msg["data"]["event_id"] = _event_id(node_state, notice_type, this_port)
    msg["data"]["ts"] = float(time.time())
    _queue_event(node_state, state_lock, push_queue, msg)


def pull_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    with state_lock:
        neighbors = node_state.get("neighbors", [])
        if not isinstance(neighbors, list):
            neighbors = []
        neighbors = [int(x) for x in neighbors if isinstance(x, int)]
        prev_score = _to_float(node_state.get("score", 0.0), 0.0)
        prev_state = str(node_state.get("protocol_state", "NORMAL")).strip().upper()
        prev_width = _to_int(node_state.get("front_width", 0), 0)
        prev_no_progress = _to_int(node_state.get("no_progress_cycles", 0), 0)
        prev_dominant = _to_int(node_state.get("dominant_sector", 0), 0)
        prev_pull_cycles = _to_int(node_state.get("pull_cycles", 0), 0)
        history = node_state.get("dominant_sector_history", [])
        if not isinstance(history, list):
            history = []
        sensor_state = str(node_state.get("sensor_state", "NORMAL")).strip().upper()
        started_ts = _to_float(node_state.get("started_ts", 0.0), 0.0)
        prev_alert_code = _to_int(node_state.get("prev_alert_code", 0), 0)
        prev_layer1_signature = str(node_state.get("last_published_layer1_signature", ""))
        prev_layer2_signature = str(node_state.get("last_published_layer2_signature", ""))

    startup_grace_sec = _to_float(config_json.get("startup_grace_sec", 4), 4.0)
    within_startup_grace = started_ts > 0.0 and (time.time() - started_ts) < startup_grace_sec

    snapshots, missing_neighbors = _probe_neighbors(
        config_json=config_json,
        node_state=node_state,
        state_lock=state_lock,
        this_port=this_port,
        neighbors=neighbors,
    )

    with state_lock:
        transitions = _classify_neighbors(node_state, neighbors, missing_neighbors)
        node_state["neighbor_states"] = transitions["next_states"]
        node_state["neighbor_miss_streak"] = transitions["next_streak"]

        incoming_front_alerts, incoming_confirmation_alerts, incoming_recovery_notices, stall_notices = _consume_events(
            config_json=config_json,
            node_state=node_state,
            this_port=this_port,
        )

    new_missing = transitions["new_missing"]
    persistent_missing = transitions["persistent_missing"]
    recovered_neighbors = transitions["recovered"]
    current_missing = transitions["current_missing"]

    new_missing_by_sector = _count_by_sector(config_json, node_state, this_port, new_missing)
    persistent_by_sector = _count_by_sector(config_json, node_state, this_port, persistent_missing)
    recovered_by_sector = _count_by_sector(config_json, node_state, this_port, recovered_neighbors)

    combined_recovery_by_sector = _empty_sector_map()
    for sector in range(1, SECTOR_COUNT + 1):
        key = str(sector)
        combined_recovery_by_sector[key] = (
            float(recovered_by_sector.get(key, 0.0)) +
            float(incoming_recovery_notices.get(key, 0.0))
        )

    front_score_by_sector = _front_scores(
        new_missing_by_sector=new_missing_by_sector,
        persistent_by_sector=persistent_by_sector,
        alert_by_sector=incoming_front_alerts,
        confirmation_by_sector=incoming_confirmation_alerts,
        recovery_by_sector=combined_recovery_by_sector,
        history=history,
    )

    dominant_sector = _dominant_sector(front_score_by_sector, prev_dominant)
    updated_history = list(history[-2:]) + ([int(dominant_sector)] if int(dominant_sector) > 0 else [])
    persistence = 1 if int(dominant_sector) > 0 and updated_history.count(int(dominant_sector)) >= 2 else 0

    front_score = 0.0
    if int(dominant_sector) > 0:
        front_score = float(front_score_by_sector.get(str(int(dominant_sector)), 0.0))

    active_sectors = _sector_keys_with_signal(
        new_missing_by_sector,
        persistent_by_sector,
        incoming_front_alerts,
        incoming_confirmation_alerts,
    )
    front_width = int(len(active_sectors))

    progression = 0
    if int(dominant_sector) > 0:
        dominant_key = str(int(dominant_sector))
        if (
            float(new_missing_by_sector.get(dominant_key, 0.0)) > 0.0 or
            float(incoming_front_alerts.get(dominant_key, 0.0)) > 0.0 or
            float(incoming_confirmation_alerts.get(dominant_key, 0.0)) > 0.0
        ):
            progression = 1

    adjacency = 1 if _has_adjacent_sectors(active_sectors) else 0
    coherence_score = int(adjacency + persistence + progression)

    cluster_bonus = 1 if _has_adjacent_sectors([_sector_for_port(config_json, node_state, this_port, p) for p in new_missing]) else 0
    impact_score = (
        3.0 * float(len(new_missing)) +
        1.0 * float(len(persistent_missing)) +
        2.0 * float(cluster_bonus)
    )

    manual_alert_bonus = 0.0
    if sensor_state == "ALERT":
        manual_alert_bonus = _to_float(config_json.get("manual_alert_impact_bonus", 2.0), 2.0)
        impact_score += manual_alert_bonus
        front_score = max(
            front_score,
            _to_float(config_json.get("front_watch_threshold", 3.0), 3.0)
        )

    front_context = bool(
        prev_state in ACTIVE_STATES or
        int(prev_width) > 0 or
        int(prev_dominant) > 0 or
        float(front_score) > 0.0 or
        float(impact_score) > 0.0 or
        len(current_missing) > 0 or
        len(recovered_neighbors) > 0 or
        int(stall_notices) > 0 or
        sensor_state in ("ALERT", "RECOVERING")
    )

    progress_event = bool(progression == 1 or sensor_state == "ALERT")
    if progress_event:
        no_progress_cycles = 0
    elif front_context:
        no_progress_cycles = int(prev_no_progress) + 1
    else:
        no_progress_cycles = 0

    stall_cycles = _to_int(config_json.get("stall_cycles", 3), 3)
    contained_cycles = _to_int(config_json.get("contained_cycles", 5), 5)

    retreat = 1 if (
        int(front_width) < int(prev_width) or
        len(recovered_neighbors) > 0 or
        sum(float(v) for v in incoming_recovery_notices.values()) > 0.0 or
        sensor_state == "RECOVERING"
    ) else 0

    bypass = 0
    if int(prev_dominant) > 0 and not progress_event:
        for sector in _adjacent_sectors(prev_dominant):
            key = str(int(sector))
            if (
                float(new_missing_by_sector.get(key, 0.0)) > 0.0 or
                float(incoming_front_alerts.get(key, 0.0)) > 0.0 or
                float(incoming_confirmation_alerts.get(key, 0.0)) > 0.0
            ):
                bypass = 1
                break

    reactivation = 1 if int(prev_no_progress) >= stall_cycles and progress_event else 0
    stall = 1 if int(no_progress_cycles) >= stall_cycles else 0
    corroboration = 1 if int(stall_notices) > 0 else 0
    arrest_score = float(stall + retreat + corroboration - bypass - reactivation)

    watch_threshold = _to_float(config_json.get("front_watch_threshold", 3.0), 3.0)
    warning_threshold = _to_float(config_json.get("front_warning_threshold", 5.0), 5.0)
    impact_threshold = _to_float(config_json.get("impact_threshold", 4.0), 4.0)
    coherence_gate = _to_int(config_json.get("coherence_gate", 2), 2)
    t_high = _to_float(node_state.get("T_high", config_json.get("T_high", 7)), config_json.get("T_high", 7))
    t_low = _to_float(node_state.get("T_low", config_json.get("T_low", 2)), config_json.get("T_low", 2))

    protocol_state = "NORMAL"
    if sensor_state == "RECOVERING":
        protocol_state = "RECOVERING"
    if float(front_score) >= watch_threshold and int(coherence_score) >= coherence_gate:
        protocol_state = "WATCH"
    if float(front_score) >= warning_threshold and int(coherence_score) >= coherence_gate:
        protocol_state = "WARNING"
    if float(impact_score) >= impact_threshold:
        protocol_state = "IMPACT"
    if int(no_progress_cycles) < stall_cycles:
        if str(prev_state) == "IMPACT" and len(current_missing) > 0:
            protocol_state = "IMPACT"
        elif str(prev_state) in ("WATCH", "WARNING") and front_context:
            protocol_state = "WARNING"
    if front_context and int(no_progress_cycles) >= stall_cycles and float(arrest_score) >= 1.0:
        protocol_state = "STALLED"
    if front_context and int(no_progress_cycles) >= contained_cycles and float(arrest_score) >= 2.0 and int(retreat) == 1 and int(bypass) == 0:
        protocol_state = "CONTAINED"
    if protocol_state == "NORMAL" and (
        sensor_state == "RECOVERING" or
        (
            prev_state in ACTIVE_STATES and
            len(recovered_neighbors) > 0 and
            float(front_score) < watch_threshold and
            float(impact_score) < impact_threshold
        )
    ):
        protocol_state = "RECOVERING"
    if sensor_state == "ALERT" and protocol_state == "NORMAL":
        protocol_state = "WATCH"

    coherence_boost = max(0.0, float(coherence_score) - 1.0)
    raw_score = max(0.0, float(front_score) + float(impact_score) + coherence_boost)
    if str(protocol_state) == "NORMAL":
        t_score = 0.0
    elif str(protocol_state) in ("WATCH", "WARNING"):
        t_score = max(float(prev_score), float(raw_score), 4.0)
    elif str(protocol_state) == "IMPACT":
        t_score = max(float(prev_score), float(raw_score), float(t_high))
    elif str(protocol_state) in ("STALLED", "CONTAINED"):
        t_score = max(2.0, float(prev_score) - 0.5)
    elif str(protocol_state) == "RECOVERING":
        t_score = max(0.0, float(prev_score) - 1.0)
    else:
        t_score = max(0.0, float(raw_score))
    score_delta = round(float(t_score) - float(prev_score), 1)

    boundary_kind = "stable"
    dfa_state = STATE_STABLE
    if protocol_state in ("WATCH", "WARNING"):
        boundary_kind = "front"
        dfa_state = STATE_VERIFY
    elif protocol_state == "IMPACT":
        boundary_kind = "impact"
        dfa_state = STATE_BOUNDARY
    elif protocol_state in ("STALLED", "CONTAINED"):
        boundary_kind = "arrest"
        dfa_state = STATE_BOUNDARY
    elif protocol_state == "RECOVERING":
        boundary_kind = "recovering"
        dfa_state = STATE_VERIFY

    alert_code = _alert_code(protocol_state, t_score)
    alert_bits = _alert_bits(alert_code)
    alert_level = _alert_level(alert_code)
    alert_delta_bits = _alert_delta_bits(alert_code, prev_alert_code)
    layer1_alert = {
        "alert_code": int(alert_code),
        "alert_bits": str(alert_bits),
        "alert_level": str(alert_level),
        "delta_bits": int(alert_delta_bits),
        "cycle": int(prev_pull_cycles) + 1,
        "score": round(float(t_score), 1),
        "state": str(protocol_state),
        "message": "L1 {} {} means something is happening".format(alert_bits, alert_level),
    }

    tomography = _compute_tomography(
        config_json=config_json,
        node_state=node_state,
        this_port=this_port,
        neighbors=neighbors,
        snapshots=snapshots,
        current_missing=current_missing,
    )
    tomography["cycle"] = int(prev_pull_cycles) + 1
    tomography["state"] = str(protocol_state)
    tomography["message"] = str(tomography.get("message", ""))

    with state_lock:
        node_state["protocol_state"] = str(protocol_state)
        node_state["boundary_kind"] = str(boundary_kind)
        node_state["dfa_state"] = int(dfa_state)
        node_state["score"] = round(float(t_score), 1)
        node_state["raw_score"] = round(float(raw_score), 1)
        node_state["score_delta"] = round(float(score_delta), 1)
        node_state["score_trend"] = _score_trend(float(score_delta))
        node_state["score_bucket"] = _score_bucket(float(t_score), float(t_low), float(t_high))

        node_state["front_score"] = round(float(front_score), 1)
        node_state["impact_score"] = round(float(impact_score), 1)
        node_state["arrest_score"] = round(float(arrest_score), 1)
        node_state["coherence_score"] = int(coherence_score)
        node_state["front_score_by_sector"] = {
            k: round(float(v), 1) for k, v in front_score_by_sector.items()
        }

        node_state["dominant_sector"] = int(dominant_sector)
        node_state["dominant_sector_history"] = [int(x) for x in updated_history[-3:]]
        node_state["active_sectors"] = [int(x) for x in active_sectors]
        node_state["front_width"] = int(front_width)
        node_state["no_progress_cycles"] = int(no_progress_cycles)

        node_state["current_missing_neighbors"] = current_missing
        node_state["new_missing_neighbors"] = new_missing
        node_state["persistent_missing_neighbors"] = persistent_missing
        node_state["recovered_neighbors"] = recovered_neighbors

        node_state["front_components"] = {
            "new_missing_by_sector": {k: float(v) for k, v in new_missing_by_sector.items()},
            "persistent_missing_by_sector": {k: float(v) for k, v in persistent_by_sector.items()},
            "fresh_alerts_by_sector": {k: float(v) for k, v in incoming_front_alerts.items()},
            "confirmations_by_sector": {k: float(v) for k, v in incoming_confirmation_alerts.items()},
            "recovery_by_sector": {k: float(v) for k, v in combined_recovery_by_sector.items()},
            "dominant_new_missing": float(new_missing_by_sector.get(str(int(dominant_sector)), 0.0)) if int(dominant_sector) > 0 else 0.0,
            "dominant_persistent_missing": float(persistent_by_sector.get(str(int(dominant_sector)), 0.0)) if int(dominant_sector) > 0 else 0.0,
            "dominant_disagreement": 1 if int(dominant_sector) > 0 and float(incoming_front_alerts.get(str(int(dominant_sector)), 0.0)) > 0.0 else 0,
            "dominant_corroboration": 1 if int(dominant_sector) > 0 and float(incoming_front_alerts.get(str(int(dominant_sector)), 0.0)) > 1.0 else 0,
            "dominant_confirmation": float(incoming_confirmation_alerts.get(str(int(dominant_sector)), 0.0)) if int(dominant_sector) > 0 else 0.0,
            "dominant_momentum": 1 if int(dominant_sector) > 0 and updated_history.count(int(dominant_sector)) >= 2 and float(front_score) > 0.0 else 0,
        }
        node_state["impact_components"] = {
            "adjacent_new_missing": int(len(new_missing)),
            "adjacent_persistent_missing": int(len(persistent_missing)),
            "cluster_bonus": int(cluster_bonus),
            "manual_alert_bonus": float(manual_alert_bonus),
        }
        node_state["coherence_components"] = {
            "adjacency": int(adjacency),
            "persistence": int(persistence),
            "progression": int(progression),
        }
        node_state["arrest_components"] = {
            "stall": int(stall),
            "retreat": int(retreat),
            "corroboration": int(corroboration),
            "bypass": int(bypass),
            "reactivation": int(reactivation),
        }
        node_state["layer1_alert"] = dict(layer1_alert)
        node_state["layer2_confirmation"] = dict(tomography)
        node_state["prev_alert_code"] = int(alert_code)
        node_state["tomo_distance_history"] = [round(float(x), 1) for x in tomography.get("history", [])]
        node_state["last_cycle_ts"] = float(time.time())
        node_state["pull_cycles"] = int(prev_pull_cycles) + 1
        if str(protocol_state) != str(prev_state):
            node_state["last_state_change_ts"] = float(time.time())

    layer1_signature = "{}|{}|{}".format(
        layer1_alert.get("alert_bits", "00"),
        layer1_alert.get("alert_level", "NORMAL"),
        layer1_alert.get("cycle", 0),
    )
    layer2_signature = "{}|{}|{}|{}|{}".format(
        tomography.get("phase", "CLEAR"),
        tomography.get("direction_label", ""),
        tomography.get("distance_hops", 99.0),
        tomography.get("speed_hops_per_cycle", 0.0),
        tomography.get("eta_cycles", 99.0),
    )

    if within_startup_grace:
        return

    if layer1_signature != prev_layer1_signature and int(alert_code) > 0:
        _publish_local_layer_message(
            node_state=node_state,
            state_lock=state_lock,
            this_port=this_port,
            push_queue=push_queue,
            neighbors=neighbors,
            notice_type="alert_state",
            payload=layer1_alert,
        )
        state_lock.acquire()
        try:
            node_state["last_published_layer1_signature"] = str(layer1_signature)
        finally:
            state_lock.release()

    if (
        layer2_signature != prev_layer2_signature and
        str(tomography.get("phase", "CLEAR")) != "CLEAR" and
        float(tomography.get("strength", 0.0)) > 0.0
    ):
        _publish_local_layer_message(
            node_state=node_state,
            state_lock=state_lock,
            this_port=this_port,
            push_queue=push_queue,
            neighbors=neighbors,
            notice_type="confirmation_notice",
            payload=tomography,
        )
        state_lock.acquire()
        try:
            node_state["last_published_layer2_signature"] = str(layer2_signature)
        finally:
            state_lock.release()

    if str(protocol_state) in ("WATCH", "WARNING", "IMPACT") and (
        progress_event or _state_rank(protocol_state) > _state_rank(prev_state)
    ):
        _publish_notice(
            config_json=config_json,
            node_state=node_state,
            state_lock=state_lock,
            this_port=this_port,
            push_queue=push_queue,
            notice_type="front_alert",
            protocol_state=protocol_state,
            score=front_score,
            dominant_sector=dominant_sector,
        )

    if str(protocol_state) in ("STALLED", "CONTAINED") and str(protocol_state) != str(prev_state):
        _publish_notice(
            config_json=config_json,
            node_state=node_state,
            state_lock=state_lock,
            this_port=this_port,
            push_queue=push_queue,
            notice_type="stall_notice",
            protocol_state=protocol_state,
            score=arrest_score,
            dominant_sector=dominant_sector,
        )

    if (
        str(protocol_state) == "RECOVERING" and
        (str(prev_state) != "RECOVERING" or len(recovered_neighbors) > 0 or sensor_state == "RECOVERING")
    ):
        _publish_notice(
            config_json=config_json,
            node_state=node_state,
            state_lock=state_lock,
            this_port=this_port,
            push_queue=push_queue,
            notice_type="recovery_notice",
            protocol_state=protocol_state,
            score=arrest_score,
            dominant_sector=dominant_sector,
        )
