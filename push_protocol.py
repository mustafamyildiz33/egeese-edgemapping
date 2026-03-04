
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


# -------------------------------------------------------------------------
# Push protocol: send queued message to targets.
# For demo + autonomy: heartbeat goes ONLY to neighbors (metadata.targets).
# -------------------------------------------------------------------------

import copy
import random
import egess_api


def push_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue, msg):
    targets = []
    md = msg.get("metadata", {})
    if isinstance(md, dict) and isinstance(md.get("targets"), list) and len(md.get("targets")) > 0:
        targets = [int(x) for x in md.get("targets") if isinstance(x, int)]
    else:
        all_nodes = list(range(config_json["base_port"], config_json["base_port"] + number_of_nodes, 1))
        other_nodes = copy.copy(all_nodes)
        if this_port in other_nodes:
            other_nodes.remove(this_port)
        k = 2 if len(other_nodes) >= 2 else len(other_nodes)
        if k > 0:
            targets = random.sample(other_nodes, k)

    for target_port in targets:
        egess_api.send_msg(config_json, node_state, state_lock, this_port, msg, target_port)

