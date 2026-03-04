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
# This file implements a test client that sends a message (in JSON format)
# to a specific node and prints the response from the node.
# -------------------------------------------------------------------------


import sys # For reading command-line arguments
import json # For encoding/decoding JSON format

def main():

    # The program takes exactly two arguments.
    if len(sys.argv) != 3:
        print("ERROR Two arguments expected.")
        # The first argument is the port of the node to send the message to.
        # The second argument is the JSON file with the message to send to the
        # node with the provided port.
        print("USAGE: {} <node_port> <trigger_msg_json_file>".format(sys.argv[0]))
        exit(1)
    
    config_file = "config.json" # All-nodes configuration file.

    node_port = int(sys.argv[1]) # Extract the port argument and convert it into integer
    trigger_msg_json_file = sys.argv[2] # Extract the second argument

    with open(config_file) as file:
        config_json = json.load(file) # Read the all-nodes configuration file into config_json object

    with open(trigger_msg_json_file) as file:
        trigger_msg = json.load(file) # Read the message to-be sent into the trigger_msg JSON object

    try:
        import requests # For sending a message via POST protocol
    except ModuleNotFoundError:
        print("ERROR: Python package 'requests' is not installed in this interpreter.")
        print("TIP: Use './.venv/bin/python trigger.py ...' or install dependencies in your active Python.")
        exit(1)

    try: # Send the POST request to the node.
        resp = requests.post("http://{}:{}/".format(config_json["base_host"], node_port), json=trigger_msg)

        # If the node replies with the success code, print the response while beautifying JSON
        if resp.status_code == 200:
            resp_json = resp.json()
            print("RESPONSE: ", json.dumps(resp_json, indent=4))
        else:
            try:
                err_json = resp.json()
            except Exception:
                err_json = {
                    "status_code": resp.status_code,
                    "text": resp.text
                }
            print("ERROR RESPONSE: ", json.dumps(err_json, indent=4))
    except requests.exceptions.ConnectionError: # If a connection error occurs
        print("ERROR: Connection error.")


# This code prevents running the initialization code if this file is imported by another module
# More information here: https://docs.python.org/3/library/__main__.html
if __name__ == "__main__":
    main()
