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
import math
import time
import egess_api


def _demo_mode() -> bool:
    return os.environ.get("DEMO_MODE", "0") == "1"


def background_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    state_lock.acquire()
    try:
        node_state["background_hits"] = int(node_state.get("background_hits", 0)) + 1

        grid_size = int(node_state.get("grid_size", 8))
        pos = node_state.get("grid_pos", [0, 0])
        x = int(pos[0])
        y = int(pos[1])

        cx = (grid_size - 1) / 2.0
        cy = (grid_size - 1) / 2.0
        radius = 2.0

        dist = math.sqrt((x - cx) ** 2 + (y - cy) ** 2)
        reading = "RED" if dist <= radius else "BLUE"

        faults = node_state.get("faults", {})
        if not isinstance(faults, dict):
            faults = {}
            node_state["faults"] = faults

        # lie_sensor flips the physical observation.
        if bool(faults.get("lie_sensor", False)):
            reading = "BLUE" if reading == "RED" else "RED"

        # flap toggles periodically to create controlled disagreement bursts.
        if bool(faults.get("flap", False)):
            period_sec = int(faults.get("period_sec", 4))
            if period_sec < 1:
                period_sec = 1
            phase = int(time.time() // period_sec) % 2
            if phase == 1:
                reading = "BLUE" if reading == "RED" else "RED"

        node_state["local_reading"] = reading

        if not _demo_mode():
            egess_api.log_current_node_state(this_port, node_state)
            egess_api.write_state_change_data_point(this_port, node_state, "background_hits")

    finally:
        state_lock.release()
