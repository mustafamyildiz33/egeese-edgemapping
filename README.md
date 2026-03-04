# EGESS
Experimental Gear for Evaluation of Swarm Systems

## Comments
Since this is a schientific tool with a lot of intricacies intended to be widely disseminated and replicated, it uses the *comment-everything* style. Each time you change something, you must also update all comments that may be affected by the change.

## Docstrings
This project uses *Google style* docstrings. Although this is a scientific project, we don't use NumPy style because it is too bulky.

## How to run it?

### Step I: Start the nodes
```
EGESS_LOG=1 ./start_nodes.sh 16
```

### Step II: Observe the logs in real time
Possibly in another terminal (but in the same directory):
```
RUN_DIR=$(ls -1t runs | head -n 1)
tail -f runs/$RUN_DIR/node_9000.log
```
This will likely be producing a lot of telemetry. `Ctrl+C` stops it.

### Step III: Send a trigger message
If needed, send a trigger message to initiate the forwarding sequence. You can repeat it as needed. For example, if you want to send the trigger message to port 9002 (the third node), then the command will be as follows:

```
./.venv/bin/python trigger.py 9002 trigger_msg.json
```

### Live Demo Controls (Visualizer)
Run the live visualizer in a separate terminal:
```
./.venv/bin/python visualize_mapping.py --base-port 9000 --n 16 --grid 4
```
Inside the map:
- Click a node in either panel: select + zoom to that node and show inspector details (`T`, trend, drivers, message counters, recent pull/push traffic).
- Press `1..6`: toggle manual disagreement slots for the selected node (each active slot contributes `+1` to `T`).
- Press `0`: clear all manual disagreement slots for the selected node.
- Press `d`: delete the selected node (inject `crash_sim`, node becomes unreachable).
- Press `a`: add/recover the selected node (disable `crash_sim`).
- Press `l`: toggle `lie_sensor` on selected node.
- Press `f`: toggle `flap` on selected node.
- Press `r` / `Esc` / `Home`: reset view.
- Press `q`: quit the visualizer.

Hands-off automatic fire/bomb demo (no key presses):
```
./.venv/bin/python visualize_mapping.py --base-port 9000 --n 16 --grid 4 --auto-demo firebomb --auto-period 10
```
This cycles through baseline -> fire/disagreement -> bomb/crash -> recovery, and auto-sends push messages.

### Step IV: Stop the nodes
Stop the network and kill all nodes using this command:
```
./stop_nodes.sh
```

### Step V: Observe the logs and the data
After running, all node logs are written to `runs/<timestamp>/node_<port>.log`. In demo mode, CSV telemetry is disabled by default (`EGESS_LOG=0` in `start_nodes.sh`); if enabled, telemetry is written to `runs/<timestamp>/data.csv`.

## A note about logging messages
All log writes should use **a single string argument**. In other words, the `print()` function must not use commas to separate fields; use `format()` instead. Otherwise, separate arguments may interleave across concurrent node logs.
