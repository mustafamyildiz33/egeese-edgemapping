#!/usr/bin/env python3
import argparse
import json
from urllib import request


def post_json(url: str, payload: dict, timeout: float = 1.0):
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--fault", type=str, required=True, choices=["crash_sim", "lie_sensor", "flap", "reset"])
    ap.add_argument("--enable", type=int, default=1)
    ap.add_argument("--period-sec", type=int, default=4)
    args = ap.parse_args()

    payload = {
        "op": "inject_fault",
        "data": {
            "fault": args.fault,
            "enable": bool(args.enable),
            "period_sec": int(args.period_sec)
        },
        "metadata": {}
    }

    url = f"http://127.0.0.1:{args.port}/"
    res = post_json(url, payload)
    print(res)


if __name__ == "__main__":
    main()
