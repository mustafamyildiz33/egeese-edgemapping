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
# This file implements the listener protocol of the node. This protocol
# is triggered each time the node receives a message (in JSON format).
# -------------------------------------------------------------------------


import time
import egess_api # Used for invoking commonly used EGESS API functions


def listener_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue, msg):
    """
    Listener protocol function.

    Args:
        config_json (dict[str, Any]): JSON object with all-nodes configuration.
        node_state (dict[str, Any]): The state of this current node.
        state_lock (threading.Lock): The lock object for thread-safety of the state.
        this_port (int): The port this node listens.
        number_of_nodes (int): The total number of nodes in the network (if known).
        push_queue (queue.Queue): The queue for messages to be pushed to other node(s).
        msg (dict[str, Any]): JSON object received via POST protocol.
    """
    if msg["op"] == "pull": # Indicates that the message is a "pull" request
        print("PULL REQUEST RECEIVED\n") # Log receiving the request
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

    elif msg["op"] == "push": # If the message is a "push" message
        if msg["metadata"]["forward_count"] < config_json["max_forwards"]:
            state_lock.acquire()
            node_state["accepted_messages"] = node_state["accepted_messages"] + 1

            if msg["metadata"]["relay"] not in node_state["known_nodes"] and msg["metadata"]["relay"] != 0:
                node_state["known_nodes"].append(msg["metadata"]["relay"])

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
                    if key not in node_state["missing_reports"]:
                        node_state["missing_reports"][key] = 0
                    node_state["missing_reports"][key] = int(node_state["missing_reports"][key]) + 1

            egess_api.write_state_change_data_point(this_port, node_state, "accepted_messages")
            egess_api.write_state_change_data_point(this_port, node_state, "known_nodes")
            state_lock.release()

            msg["metadata"]["relay"] = this_port
            msg["metadata"]["forward_count"] = msg["metadata"]["forward_count"] + 1
            push_queue.put(msg)

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
        print("ERROR: listener_protocol: unknown type of message: {}\n".format(msg["op"]))
        return {
            "op": "receipt",
            "data": {
                "success": False,
                "message": "unknown operation"
            },
            "metadata": {}
        }
