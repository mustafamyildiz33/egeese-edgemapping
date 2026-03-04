# EGESS - Experimental Gear for Evaluation of Swarm Systems
# Copyright (C) 2026  Nick Ivanov and ACSUS Lab <ivanov@rowan.edu>

import random
import time
import egess_api


STATE_STABLE = 1
STATE_VERIFY = 2
STATE_BOUNDARY = 3

BUCKET_COLD = 0
BUCKET_WARM = 1
BUCKET_HOT = 2
BUCKET_BOIL = 3


def _score_bucket(score):
    if score <= 1:
        return BUCKET_COLD
    if score <= 3:
        return BUCKET_WARM
    if score <= 5:
        return BUCKET_HOT
    return BUCKET_BOIL


def pull_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    state_lock.acquire()
    dfa_state = int(node_state.get("dfa_state", STATE_STABLE))
    neighbors = node_state.get("neighbors", [])
    score = int(node_state.get("score", 0))
    T_high = int(node_state.get("T_high", int(config_json.get("T_high", 4))))
    T_low = int(node_state.get("T_low", int(config_json.get("T_low", 2))))
    rearm_locked = bool(node_state.get("rearm_locked", False))
    verify_lock = bool(node_state.get("verify_lock", False))
    cooldown_until = int(node_state.get("verify_cooldown_until", 0))
    false_cnt = int(node_state.get("false_verify_count", 0))
    missing_debounce_m = int(node_state.get("missing_debounce_m", int(config_json.get("missing_debounce_m", 3))))
    state_lock.release()

    now_tick = int(time.time())

    if dfa_state == STATE_VERIFY:
        return

    if verify_lock:
        return

    if now_tick < cooldown_until:
        return

    if rearm_locked:
        if score <= T_low:
            state_lock.acquire()
            node_state["rearm_locked"] = False
            state_lock.release()
        else:
            return

    if score < T_high:
        return

    jitter_max = int(config_json.get("verify_jitter_max_ticks", 2))
    if jitter_max > 0:
        time.sleep(random.uniform(0.0, float(jitter_max)))

    state_lock.acquire()
    node_state["dfa_state"] = STATE_VERIFY
    node_state["verify_lock"] = True
    state_lock.release()

    confirmed_missing = []
    for nport in neighbors:
        msg = {
            "op": "pull",
            "data": {"kind": "neighbor_status"},
            "metadata": {"origin": this_port}
        }
        resp = egess_api.send_msg(config_json, node_state, state_lock, this_port, msg, int(nport))
        if not resp or resp.get("op") != "receipt":
            confirmed_missing.append(int(nport))
            continue
        data = resp.get("data", {})
        ok = bool(data.get("success", False))
        if not ok:
            confirmed_missing.append(int(nport))

    if len(confirmed_missing) > 0:
        state_lock.acquire()
        node_state["dfa_state"] = STATE_BOUNDARY
        node_state["verify_lock"] = False
        node_state["rearm_locked"] = True
        node_state["boundary_edges"] = confirmed_missing
        node_state["score_bucket"] = _score_bucket(int(node_state.get("score", 0)))
        state_lock.release()

        alert_id = "{}-{}".format(this_port, int(time.time() * 1000))
        ttl = int(config_json.get("alert_ttl", 8))

        alert_msg = {
            "op": "push",
            "data": {
                "type": "alert",
                "alert_id": alert_id,
                "ttl": ttl,
                "kind": "boundary_confirmed",
                "missing_neighbors": confirmed_missing
            },
            "metadata": {
                "origin": this_port,
                "relay": this_port,
                "forward_count": 0
            }
        }
        push_queue.put(alert_msg)

    else:
        false_cnt += 1
        base_cd = int(config_json.get("verify_cooldown_base_ticks", 8))
        max_cd = int(config_json.get("verify_cooldown_max_ticks", 80))
        cooldown = base_cd * (2 ** (false_cnt - 1))
        if cooldown > max_cd:
            cooldown = max_cd

        state_lock.acquire()
        node_state["dfa_state"] = STATE_STABLE
        node_state["verify_lock"] = False
        node_state["rearm_locked"] = True
        node_state["false_verify_count"] = false_cnt
        node_state["verify_cooldown_until"] = int(time.time()) + int(cooldown)
        node_state["score_bucket"] = _score_bucket(int(node_state.get("score", 0)))
        state_lock.release()
