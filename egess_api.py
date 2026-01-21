import json
import time
import requests

def log_new_node_state(this_port, apriori_node_state, aposteriori_node_state):
    print("NODE STATE CHANGED (NODE {}):\nAPRIORI: {}\nAPOSTERIORI: {}\n"
        .format(
                this_port,
                json.dumps(apriori_node_state),
                json.dumps(aposteriori_node_state)
            )
        )


def log_current_node_state(this_port, node_state):
    print("NODE STATE CHANGED (NODE {}):\nSTATE: {}\n"
        .format(
                this_port,
                json.dumps(node_state)
            )
        )


def write_data_point(this_port, logtype, message):
    data_file = "data.csv"
    with open(data_file, "a") as f:
        f.write("{},{},{},{}\n".format(this_port, time.time(), logtype, message))
        f.close()


def send_msg(config_json, node_state, state_lock, this_port, msg, target_port):
    try:
        host_url = "http://" + config_json["base_host"]
        resp = requests.post("{}:{}/".format(host_url, target_port), json=msg)

        if resp.status_code == 200:
            # write_data_point(this_port, "SEND_MSG", msg["metadata"]["forward_count"])
            print("MESSAGE SENT ({} -> {}): {}".format(this_port, target_port, msg))
            resp_json = resp.json()
        else:
            print("ERROR: send_msg: return code is not 200")
        
    except requests.exceptions.ConnectionError:
        print("ERROR: Connection error")

