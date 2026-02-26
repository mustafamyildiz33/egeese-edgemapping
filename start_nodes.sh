#!/bin/bash

pkill -f node.py

mkdir -p backupdata
mv data.csv backupdata/data-`date +%s`.csv 2>/dev/null
touch data.csv

echo "" > run.log

N=64
PORT=9000

PY="./.venv/bin/python"
if [ ! -x "$PY" ]; then
  PY="python3"
fi

for ((i=1; i<=N; i++)); do
    $PY -u node.py $PORT $N  >> run.log  2>&1 &
    PORT=$((PORT + 1))
done

ps -ef | grep node.py | grep -v grep
