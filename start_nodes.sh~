#!/bin/bash

# This script starts N swarm localhost (127.0.0.1) nodes listening
# ports 9000, 9000+1, 9000+2, ..., 9000+N. All messages produced
# by all nodes are logged in run.log. The previous session's
# data points are moved to a backup file, and a fresh data.csv
# file is created by this script. At the end, the script outputs the
# list of swarm node processes under the current terminal.

# Kill all running nodes
pkill -f node.py

# Backup the data from the previous (most recent) run in the backupdata folder
mkdir -p backupdata
mv data.csv backupdata/data-`date +%s`.csv
touch data.csv

# Erase the log from the most recent run
echo "" > run.log

# Number of nodes
N=5

# We start counting ports from 9000
PORT=9000

# Iterate from 1 (inclusive) until N (inclusive)
for ((i=1; i<=N; i++)); do
    # Start each node one by one forcing the stdout and stderr streams to be unbuffered
    # and appending both stderr and stdout to run.log
    python3 -u node.py $PORT $N  >> run.log  2>&1 &
    # Increment the current port number
    PORT=$((PORT + 1))
done

# List the processes currently running nodes
ps -ef | grep node.py | grep -v grep
