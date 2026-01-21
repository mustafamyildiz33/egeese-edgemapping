import time
import egess_api

def daemon_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
     time.sleep(5)
     state_lock.acquire()
     node_state["daemon_hits"] = node_state["daemon_hits"] + 1
     egess_api.log_current_node_state(this_port, node_state)
     state_lock.release()
