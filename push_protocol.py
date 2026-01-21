import time
import copy
import random
import requests
import json
import egess_api

def push_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue, msg):
    if msg == None:
        return
    else:
        all_nodes = list(range(config_json["base_port"], config_json["base_port"] + number_of_nodes, 1))
        other_nodes = copy.copy(all_nodes)
        other_nodes.remove(this_port)
        node_sample = random.sample(other_nodes, 2)
    
        for target_port in node_sample:
            print("MESSAGE FORWARDED {} {}\n".format(str(this_port), str(target_port)))
            egess_api.send_msg(config_json, node_state, state_lock, this_port, msg, target_port)

