import time
import egess_api
import random
import copy

def pull_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    time.sleep(10)

    msg = {
        "op": "pull",
        "data": {
            "foo": "bar"
        },
        "metadata": {
            "fooo": "baar"
        }
    }

    all_nodes = list(range(config_json["base_port"], config_json["base_port"] + number_of_nodes, 1))
    other_nodes = copy.copy(all_nodes)
    other_nodes.remove(this_port)
    node_sample = random.sample(other_nodes, 1)

    egess_api.send_msg(config_json, node_state, state_lock, this_port, msg, node_sample[0])
    