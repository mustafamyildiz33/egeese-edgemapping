#!/usr/bin/env python3
"""
EGESS Traffic Monitor
Polls running EGESS nodes and displays a live-updating table.

Usage:
  python3 egess_monitor.py                               # 64 nodes from port 9000
  python3 egess_monitor.py --n 36 --base 9000           # custom node count
  python3 egess_monitor.py --refresh 1.0                # faster refresh
  python3 egess_monitor.py --compact                    # only show non-NORMAL nodes
  python3 egess_monitor.py --demo spread --compact      # corner spread demo
  python3 egess_monitor.py --demo tornado --compact     # center tornado demo
"""

import argparse
import json
import math
import os
import time
import urllib.request


SQRT3 = math.sqrt(3.0)


def post_json(port, payload, timeout=1.0):
    url = "http://127.0.0.1:{}/".format(port)
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def pull_state(port, timeout=1.0):
    data = post_json(
        port,
        {
            "op": "pull",
            "data": {"kind": "neighbor_status"},
            "metadata": {"origin": "monitor"},
        },
        timeout=timeout,
    )
    if (
        isinstance(data, dict)
        and data.get("op") == "receipt"
        and data.get("data", {}).get("success")
    ):
        return data["data"].get("node_state", {})
    return None


def inject_state(port, sensor_state, timeout=1.0):
    return post_json(
        port,
        {
            "op": "inject_state",
            "data": {"sensor_state": str(sensor_state).strip().upper()},
            "metadata": {"origin": "monitor_demo"},
        },
        timeout=timeout,
    )


def inject_fault(port, fault, enable=True, timeout=1.0):
    return post_json(
        port,
        {
            "op": "inject_fault",
            "data": {
                "fault": str(fault),
                "enable": bool(enable),
                "period_sec": 4,
            },
            "metadata": {"origin": "monitor_demo"},
        },
        timeout=timeout,
    )


def reset_port(port, timeout=1.0):
    inject_fault(port, "reset", True, timeout=timeout)
    inject_state(port, "NORMAL", timeout=timeout)


def reset_ports(ports, timeout=0.6):
    for port in ports:
        reset_port(port, timeout=timeout)


def clr():
    os.system("cls" if os.name == "nt" else "clear")


def c(text, code):
    return "\033[{}m{}\033[0m".format(code, text)


STATE_COLOR = {
    "NORMAL": "34",
    "WATCH": "33",
    "WARNING": "33;1",
    "IMPACT": "31;1",
    "STALLED": "35",
    "CONTAINED": "35;1",
    "RECOVERING": "32",
    "OFFLINE": "90",
}


def slope_sym(val):
    v = float(val)
    if v >= 2:
        return c("▲▲", "31;1")
    if v >= 1:
        return c("▲ ", "33")
    if v > 0:
        return c("△ ", "33")
    if v == 0:
        return c("— ", "90")
    if v > -1:
        return c("▽ ", "32")
    return c("▼ ", "32;1")


def _fmt_list(items, limit=4):
    if not isinstance(items, list) or len(items) == 0:
        return "[]"
    head = [str(x) for x in items[:limit]]
    if len(items) > limit:
        head.append("..+{}".format(len(items) - limit))
    return "[" + ",".join(head) + "]"


def _fit_plain(text, width):
    s = str(text)
    if len(s) > width:
        return s[: max(0, width - 1)] + "…"
    return s.ljust(width)


def hex_xy(r, cidx):
    return {
        "x": SQRT3 * (cidx + (0.5 if r % 2 == 1 else 0.0)),
        "y": 1.5 * r,
    }


def make_nodes(base, n):
    g = int(round(math.sqrt(float(n))))
    if g * g != n:
        raise ValueError("--demo currently requires a square node count like 16, 36, or 64")
    nodes = []
    for i in range(n):
        r = i // g
        cidx = i % g
        xy = hex_xy(r, cidx)
        nodes.append(
            {
                "port": base + i,
                "row": r,
                "col": cidx,
                "x": xy["x"],
                "y": xy["y"],
            }
        )
    return nodes, g


def layers_from(start_port, nodes):
    start = None
    for node in nodes:
        if node["port"] == start_port:
            start = node
            break
    if start is None:
        return []

    grouped = {}
    for node in nodes:
        dx = node["x"] - start["x"]
        dy = node["y"] - start["y"]
        dist = int(math.floor(math.sqrt(dx * dx + dy * dy) / 1.8))
        grouped.setdefault(dist, []).append(node["port"])
    return [grouped[k] for k in sorted(grouped.keys())]


def corner_script(base, n):
    nodes, _ = make_nodes(base, n)
    layers = layers_from(base, nodes)
    mx = min(len(layers), 5)
    steps = [{"a": [], "k": [], "r": [], "lbl": "Baseline — all stable, T = 0"}]
    steps.append({"a": layers[0] if len(layers) > 0 else [], "k": [], "r": [], "lbl": "Disturbance at corner"})
    for i in range(1, mx):
        steps.append(
            {
                "a": layers[i] if i < len(layers) else [],
                "k": layers[i - 1] if i - 1 < len(layers) else [],
                "r": [],
                "lbl": "Shell {} alerted, shell {} offline".format(i, i - 1),
            }
        )
    steps.extend(
        [
            {"a": [], "k": [], "r": [], "lbl": "No new losses — containment building"},
            {"a": [], "k": [], "r": [], "lbl": "Front arrested — CONTAINED"},
        ]
    )
    for i in range(mx):
        steps.append(
            {
                "a": [],
                "k": [],
                "r": layers[i] if i < len(layers) else [],
                "lbl": "Recovery — shell {}".format(i),
            }
        )
    steps.append({"a": [], "k": [], "r": [], "clr": True, "lbl": "Baseline restored"})
    return steps


def tornado_script(base, n):
    nodes, g = make_nodes(base, n)
    center = base + (g // 2) * g + (g // 2)
    layers = layers_from(center, nodes)
    return [
        {"a": [], "k": [], "r": [], "lbl": "Baseline — T = 0"},
        {"a": layers[0] if len(layers) > 0 else [], "k": [], "r": [], "lbl": "Sudden disturbance at center"},
        {
            "a": layers[1] if len(layers) > 1 else [],
            "k": layers[0] if len(layers) > 0 else [],
            "r": [],
            "lbl": "Center destroyed, ring 1 hit",
        },
        {"a": [], "k": layers[1] if len(layers) > 1 else [], "r": [], "lbl": "Ring 1 destroyed"},
        {"a": [], "k": [], "r": [], "lbl": "Destruction stops — containment building"},
        {"a": [], "k": [], "r": [], "lbl": "Front arrested — CONTAINED"},
        {"a": [], "k": [], "r": layers[0] if len(layers) > 0 else [], "lbl": "Center recovering"},
        {"a": [], "k": [], "r": layers[1] if len(layers) > 1 else [], "lbl": "Ring 1 recovering"},
        {"a": [], "k": [], "r": [], "clr": True, "lbl": "Baseline restored"},
    ]


def build_demo_script(name, base, n):
    if name == "spread":
        return corner_script(base, n)
    if name == "tornado":
        return tornado_script(base, n)
    raise ValueError("unknown demo: {}".format(name))


def apply_demo_step(step, all_ports, prev_alerts):
    for port in sorted(prev_alerts):
        inject_state(port, "NORMAL", timeout=0.5)

    actions = []

    if step.get("clr"):
        reset_ports(all_ports, timeout=0.5)
        actions.append("reset all")
        return set(), actions

    for port in step.get("k", []) or []:
        inject_fault(port, "crash_sim", True, timeout=0.5)
        actions.append("kill {}".format(port))

    for port in step.get("a", []) or []:
        inject_fault(port, "reset", True, timeout=0.5)
        inject_state(port, "ALERT", timeout=0.5)
        actions.append("alert {}".format(port))

    for port in step.get("r", []) or []:
        inject_fault(port, "reset", True, timeout=0.5)
        inject_state(port, "RECOVERING", timeout=0.5)
        actions.append("recover {}".format(port))

    return set(step.get("a", []) or []), actions


def run(base, n, refresh, compact, demo=None, step_interval=6.0):
    start = time.time()
    ports = [base + i for i in range(n)]

    demo_steps = None
    demo_idx = -1
    demo_label = ""
    next_step_time = None
    prev_alerts = set()
    last_actions = []
    demo_done = False

    if demo:
        demo_steps = build_demo_script(demo, base, n)
        reset_ports(ports, timeout=0.5)
        demo_idx = 0
        demo_label = demo_steps[0].get("lbl", "Baseline")
        next_step_time = time.time() + step_interval
        last_actions = ["reset all"]

    print(c("\n  EGESS Traffic Monitor", "37;1"))
    banner = "  {} nodes on ports {}-{} | refresh {:.1f}s | Ctrl+C to stop".format(
        n, base, base + n - 1, refresh
    )
    if demo:
        banner += " | demo={} step {:.0f}s".format(demo, step_interval)
    print(c(banner + "\n", "90"))
    time.sleep(0.4)

    while True:
        now = time.time()
        if demo_steps and not demo_done and next_step_time is not None and now >= next_step_time:
            demo_idx += 1
            if demo_idx < len(demo_steps):
                step = demo_steps[demo_idx]
                prev_alerts, last_actions = apply_demo_step(step, ports, prev_alerts)
                demo_label = step.get("lbl", "")
                if demo_idx >= len(demo_steps) - 1:
                    demo_done = True
                    next_step_time = None
                else:
                    next_step_time = now + step_interval

        states = {}
        for port in ports:
            st = pull_state(port)
            if st is not None:
                states[port] = st

        elapsed = time.time() - start
        W = 94

        lines = [""]
        lines.append(c("┌" + "─" * W + "┐", "37;1"))

        max_cyc = 0
        for st in states.values():
            max_cyc = max(max_cyc, int(st.get("pull_cycles", 0)))

        title = "  EGESS Traffic Monitor  │  {} nodes  │  cycle ~{}  │  {:.0f}s".format(
            n, max_cyc, elapsed
        )
        lines.append(c("│", "37;1") + c(title[:W].ljust(W), "37;1") + c("│", "37;1"))

        if demo_steps:
            if next_step_time is None:
                demo_tail = "complete"
            else:
                demo_tail = "next in {}s".format(max(0, int(round(next_step_time - now))))
            demo_hdr = "  DEMO: {}  [step {}/{} | {}]".format(
                demo_label,
                max(1, demo_idx + 1),
                len(demo_steps),
                demo_tail,
            )
            lines.append(c("│", "37;1") + c(demo_hdr[:W].ljust(W), "36") + c("│", "37;1"))

        lines.append(
            c(
                "├"
                + "─" * 6
                + "┬"
                + "─" * 12
                + "┬"
                + "─" * 6
                + "┬"
                + "─" * 8
                + "┬"
                + "─" * 8
                + "┬"
                + "─" * 8
                + "┬"
                + "─" * 8
                + "┬"
                + "─" * 8
                + "┬"
                + "─" * 26
                + "┤",
                "37;1",
            )
        )

        hdr = (
            c("│", "37;1")
            + " Port "
            + c("│", "37;1")
            + " State      "
            + c("│", "37;1")
            + " T    "
            + c("│", "37;1")
            + " Slope  "
            + c("│", "37;1")
            + " Front  "
            + c("│", "37;1")
            + " Impact "
            + c("│", "37;1")
            + " PullRx "
            + c("│", "37;1")
            + " PushRx "
            + c("│", "37;1")
            + " Missing                  "
            + c("│", "37;1")
        )
        lines.append(hdr)
        lines.append(
            c(
                "├"
                + "─" * 6
                + "┼"
                + "─" * 12
                + "┼"
                + "─" * 6
                + "┼"
                + "─" * 8
                + "┼"
                + "─" * 8
                + "┼"
                + "─" * 8
                + "┼"
                + "─" * 8
                + "┼"
                + "─" * 8
                + "┼"
                + "─" * 26
                + "┤",
                "37;1",
            )
        )

        cnt = {}
        t_pull_rx = 0
        t_push_rx = 0
        t_pull_tx = 0
        t_push_tx = 0
        offline = []

        for port in ports:
            st = states.get(port)

            if st is None:
                offline.append(port)
                cnt["OFFLINE"] = cnt.get("OFFLINE", 0) + 1
                if not compact:
                    lines.append(
                        c("│", "37;1")
                        + c(str(port).center(6), "90")
                        + c("│", "37;1")
                        + c("OFFLINE".center(12), "90")
                        + c("│", "37;1")
                        + c("—".center(6), "90")
                        + c("│", "37;1")
                        + c("—".center(8), "90")
                        + c("│", "37;1")
                        + c("—".center(8), "90")
                        + c("│", "37;1")
                        + c("—".center(8), "90")
                        + c("│", "37;1")
                        + c("—".center(8), "90")
                        + c("│", "37;1")
                        + c("—".center(8), "90")
                        + c("│", "37;1")
                        + c("(unreachable)".ljust(26), "90")
                        + c("│", "37;1")
                    )
                continue

            pstate = str(st.get("protocol_state", "NORMAL")).upper()
            cnt[pstate] = cnt.get(pstate, 0) + 1
            t_score = float(st.get("score", 0))
            delta = float(st.get("score_delta", 0))
            fs = float(st.get("front_score", 0))
            ims = float(st.get("impact_score", 0))
            ctrs = st.get("msg_counters", {})
            pr = int(ctrs.get("pull_rx", 0))
            psr = int(ctrs.get("push_rx", 0))
            pt = int(ctrs.get("pull_tx", 0))
            pst = int(ctrs.get("push_tx", 0))
            t_pull_rx += pr
            t_push_rx += psr
            t_pull_tx += pt
            t_push_tx += pst

            miss = st.get("current_missing_neighbors", [])
            if not isinstance(miss, list):
                miss = []
            miss_str = ",".join(str(m) for m in miss[:3])
            if len(miss) > 3:
                miss_str += "..+" + str(len(miss) - 3)
            if not miss_str:
                miss_str = "—"

            sc = STATE_COLOR.get(pstate, "0")
            is_active = t_score > 0 or pstate != "NORMAL"

            if compact and not is_active:
                continue

            if is_active:
                lines.append(
                    c("│", "37;1")
                    + c(str(port).center(6), "37;1")
                    + c("│", "37;1")
                    + " "
                    + c(pstate.ljust(11), sc)
                    + c("│", "37;1")
                    + c("{:5.1f}".format(t_score), sc)
                    + " "
                    + c("│", "37;1")
                    + " "
                    + slope_sym(delta)
                    + "{:+.1f}".format(delta).ljust(4)
                    + c("│", "37;1")
                    + c("{:6.1f}".format(fs), "33")
                    + "  "
                    + c("│", "37;1")
                    + c("{:6.1f}".format(ims), "31")
                    + "  "
                    + c("│", "37;1")
                    + str(pr).center(8)
                    + c("│", "37;1")
                    + str(psr).center(8)
                    + c("│", "37;1")
                    + " "
                    + miss_str.ljust(25)
                    + c("│", "37;1")
                )
            else:
                lines.append(
                    c("│", "37;1")
                    + c(str(port).center(6), "90")
                    + c("│", "37;1")
                    + c(" NORMAL".ljust(12), "90")
                    + c("│", "37;1")
                    + c("  0.0", "90")
                    + " "
                    + c("│", "37;1")
                    + c("  —   ", "90")
                    + "  "
                    + c("│", "37;1")
                    + c("   0.0", "90")
                    + "  "
                    + c("│", "37;1")
                    + c("   0.0", "90")
                    + "  "
                    + c("│", "37;1")
                    + c(str(pr).center(8), "90")
                    + c("│", "37;1")
                    + c(str(psr).center(8), "90")
                    + c("│", "37;1")
                    + c(" —".ljust(26), "90")
                    + c("│", "37;1")
                )

        lines.append(
            c(
                "├"
                + "─" * 6
                + "┴"
                + "─" * 12
                + "┴"
                + "─" * 6
                + "┴"
                + "─" * 8
                + "┴"
                + "─" * 8
                + "┴"
                + "─" * 8
                + "┴"
                + "─" * 8
                + "┴"
                + "─" * 8
                + "┴"
                + "─" * 26
                + "┤",
                "37;1",
            )
        )

        parts = []
        for state in ("NORMAL", "WATCH", "WARNING", "IMPACT", "STALLED", "CONTAINED", "RECOVERING", "OFFLINE"):
            value = cnt.get(state, 0)
            if value > 0:
                parts.append(c("{}={}".format(state, value), STATE_COLOR.get(state, "0")))
        lines.append(c("│", "37;1") + "  " + "  ".join(parts).ljust(W - 2) + c("│", "37;1"))

        traffic = "  Traffic: pull tx={} rx={} | push tx={} rx={}".format(
            t_pull_tx, t_pull_rx, t_push_tx, t_push_rx
        )
        lines.append(c("│", "37;1") + traffic.ljust(W) + c("│", "37;1"))

        if offline:
            off_str = ",".join(str(p) for p in offline[:8])
            if len(offline) > 8:
                off_str += " ...({} total)".format(len(offline))
            lines.append(
                c("│", "37;1")
                + c("  Offline: [{}]".format(off_str).ljust(W), "90")
                + c("│", "37;1")
            )

        nw = cnt.get("WATCH", 0) + cnt.get("WARNING", 0)
        ni = cnt.get("IMPACT", 0)
        nc = cnt.get("CONTAINED", 0) + cnt.get("STALLED", 0)
        nr = cnt.get("RECOVERING", 0)
        no = cnt.get("OFFLINE", 0)

        if nr > 0 and ni == 0:
            hz = c("● RECOVERING", "32;1") + " — {} recovering".format(nr)
        elif nc > 0 and ni == 0:
            hz = c("● CONTAINED", "35;1") + " — {} at boundary".format(nc)
        elif ni > 0 and nw > 0:
            hz = c("● ACTIVE SPREAD", "31;1") + " — {} impact, {} front, {} offline".format(ni, nw, no)
        elif ni > 0:
            hz = c("● LOCAL IMPACT", "31;1") + " — {} impacted".format(ni)
        elif nw > 0 and no > 0:
            hz = c("● APPROACHING", "33;1") + " — {} sensing, {} offline".format(nw, no)
        elif no > 0:
            hz = c("● OFFLINE", "90") + " — {} unreachable".format(no)
        else:
            hz = c("● CLEAR", "32") + " — no hazard activity"

        lines.append(c("│", "37;1") + "  " + hz.ljust(W + 20) + c("│", "37;1"))

        score_lines = []
        message_lines = []
        active_ports = []
        for port in ports:
            st = states.get(port)
            if st is None:
                continue
            pstate = str(st.get("protocol_state", "NORMAL")).upper()
            t_score = float(st.get("score", 0))
            if pstate == "NORMAL" and t_score <= 0:
                continue
            active_ports.append(port)

            new_missing = st.get("new_missing_neighbors", [])
            persistent_missing = st.get("persistent_missing_neighbors", [])
            recovered_neighbors = st.get("recovered_neighbors", [])
            front_score = float(st.get("front_score", 0))
            impact_score = float(st.get("impact_score", 0))
            arrest_score = float(st.get("arrest_score", 0))
            coherence_score = int(st.get("coherence_score", 0))
            no_progress = int(st.get("no_progress_cycles", 0))
            impact_components = st.get("impact_components", {})
            arrest_components = st.get("arrest_components", {})

            detail = "{} {:10} T={} | front={} impact={} arrest={} coh={}/3 noProg={}".format(
                port,
                pstate,
                round(t_score, 1),
                round(front_score, 1),
                round(impact_score, 1),
                round(arrest_score, 1),
                coherence_score,
                no_progress,
            )
            if new_missing:
                detail += " | newMiss={}".format(_fmt_list(new_missing))
            if persistent_missing:
                detail += " | persistMiss={}".format(_fmt_list(persistent_missing))
            if recovered_neighbors:
                detail += " | recovered={}".format(_fmt_list(recovered_neighbors))

            adj_new = int(impact_components.get("adjacent_new_missing", 0))
            adj_persist = int(impact_components.get("adjacent_persistent_missing", 0))
            cluster_bonus = int(impact_components.get("cluster_bonus", 0))
            if adj_new > 0 or adj_persist > 0 or cluster_bonus > 0:
                detail += " | impact:{}new+{}persist+{}cluster".format(
                    adj_new, adj_persist, cluster_bonus
                )

            stall = int(arrest_components.get("stall", 0))
            retreat = int(arrest_components.get("retreat", 0))
            bypass = int(arrest_components.get("bypass", 0))
            if stall or retreat or bypass:
                detail += " | arrest:stall={}+retreat={}-bypass={}".format(
                    stall, retreat, bypass
                )

            score_lines.append(detail)

            recent_msgs = st.get("recent_msgs", [])
            if isinstance(recent_msgs, list) and recent_msgs:
                for msg in recent_msgs[-2:]:
                    message_lines.append("{} {:4} {}".format(port, pstate[:4], str(msg)))

        if score_lines or message_lines:
            lines.append(c("├" + "─" * W + "┤", "37;1"))
            lines.append(
                c("│", "37;1")
                + c(_fit_plain("  Score Details (active nodes):", W), "37;1")
                + c("│", "37;1")
            )
            for detail in score_lines[:10]:
                lines.append(c("│", "37;1") + _fit_plain("  " + detail, W) + c("│", "37;1"))

            if message_lines:
                lines.append(
                    c("│", "37;1")
                    + c(_fit_plain("  Recent Messages:", W), "90")
                    + c("│", "37;1")
                )
                for msg in message_lines[-8:]:
                    lines.append(c("│", "37;1") + _fit_plain("  " + msg, W) + c("│", "37;1"))

        lines.append(c("└" + "─" * W + "┘", "37;1"))

        if demo_steps:
            lines.append(c("  Last injection:", "37;1"))
            if last_actions:
                for action in last_actions[:8]:
                    lines.append(c("    {}".format(action), "90"))
            else:
                lines.append(c("    (no injection this step)", "90"))

        lines.append(c("  Ctrl+C to stop{}".format(" and reset nodes" if demo_steps else ""), "90"))

        clr()
        print("\n".join(lines))

        if len(states) == 0:
            print(c("\n  ⚠  No nodes responding. Start them first.", "33;1"))

        time.sleep(refresh)


def main():
    parser = argparse.ArgumentParser(description="EGESS Traffic Monitor")
    parser.add_argument("--base", type=int, default=9000)
    parser.add_argument("--n", type=int, default=64)
    parser.add_argument("--refresh", type=float, default=2.0)
    parser.add_argument("--compact", action="store_true", help="Only show non-NORMAL nodes")
    parser.add_argument("--demo", choices=("spread", "tornado"), help="Run a built-in demo against real nodes")
    parser.add_argument("--step-interval", type=float, default=6.0, help="Seconds between demo injections")
    args = parser.parse_args()

    ports = [args.base + i for i in range(args.n)]
    try:
        run(args.base, args.n, args.refresh, args.compact, demo=args.demo, step_interval=args.step_interval)
    except KeyboardInterrupt:
        if args.demo:
            reset_ports(ports, timeout=0.5)
            print(c("\n\n  Demo stopped. Nodes reset to NORMAL.\n", "90"))
        else:
            print(c("\n\n  Monitor stopped.\n", "90"))


if __name__ == "__main__":
    main()
