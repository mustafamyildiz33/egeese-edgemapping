#!/bin/bash

pkill -f node.py

mv data.csv backupdata/data-`date +%s`.csv
touch data.csv

echo "" > run.log

N=5
PORT=9000

for ((i=1; i<=N; i++)); do
    python3 -u node.py $PORT $N  >> run.log  2>&1 &
    PORT=$((PORT + 1))
done

ps