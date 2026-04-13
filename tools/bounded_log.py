#!/usr/bin/env python3
"""Write stdin to a log file while keeping the file size bounded.

The paper eval demos can start dozens of noisy localhost nodes. This helper
keeps enough tail context for live demos and Node Spotlight without allowing
per-node logs to grow until the disk fills.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _to_int(value: str, fallback: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(fallback)


def _trim_file(path: Path, max_bytes: int) -> None:
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return
    if size <= max_bytes:
        return
    with path.open("rb") as handle:
        handle.seek(max(0, size - max_bytes))
        payload = handle.read()
    path.write_bytes(payload)


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: bounded_log.py <path> [max_bytes]", file=sys.stderr)
        return 2

    path = Path(sys.argv[1])
    max_bytes = max(4096, _to_int(sys.argv[2], 65536) if len(sys.argv) > 2 else 65536)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")

    pending = 0
    with path.open("ab", buffering=0) as handle:
        for chunk in sys.stdin.buffer:
            handle.write(chunk)
            pending += len(chunk)
            if pending >= max_bytes:
                pending = 0
                handle.flush()
                try:
                    os.fsync(handle.fileno())
                except OSError:
                    pass
                _trim_file(path, max_bytes)
    _trim_file(path, max_bytes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
