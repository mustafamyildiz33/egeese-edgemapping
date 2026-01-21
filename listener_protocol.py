import time
import json
import copy
import egess_api


def listener_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue, msg):
    # IMPORTANT: KEEP THIS SAFEGUARD INTACT! Otherwise, the network might be inundated with runaway transactions

    if msg["op"] == "pull":
        print("PULL REQUEST RECEIVED")
        egess_api.write_data_point(this_port, "pull_request_received", str(0))
        return {
            "op": "receipt",
            "data": {},
            "metadata": {}
        }
    else:
        if msg["metadata"]["forward_count"] < config_json["max_forwards"]:
            msg["metadata"]["forward_count"] = msg["metadata"]["forward_count"] + 1
            
            state_lock.acquire()
            node_state["accepted_messages"] = node_state["accepted_messages"] + 1

            if msg["metadata"]["relay"] not in node_state["known_nodes"] and msg["metadata"]["relay"] != 0:
                node_state["known_nodes"].append(msg["metadata"]["relay"])

            state_lock.release()

            msg["metadata"]["relay"] = this_port

            push_queue.put(msg)

            return {
                "enqueued": True
            }
        else:
            return {
                "enqueued": False
            }