import csv
import matplotlib.pyplot as plt

# We are plotting accepted_messages over time for node 9000.
# so a step plot is the correct representation.

node_id = "9000"
times = []
accepted_messages = []

with open("data.csv", "r") as f:
    reader = csv.reader(f, delimiter=';')
    for row in reader:
        if len(row) < 4:
            continue

        node, timestamp, event_type, value = row

        if node == node_id and "accepted_messages" in value:
            msg_count = int(value.split("=")[1])
            times.append(float(timestamp))
            accepted_messages.append(msg_count)

# Normalize time so the first event starts at t = 0
t0 = times[0]
times = [t - t0 for t in times]

# Plot using a step function (correct for discrete events)
plt.figure()
plt.step(times, accepted_messages, where="post")
plt.xlabel("time (s)")
plt.ylabel("accepted_messages")
plt.title("Node 9000: accepted_messages vs time")
plt.tight_layout()

# Save outputs
plt.savefig("fig1.pdf")
plt.savefig("fig1.png", dpi=200)

print("done")
