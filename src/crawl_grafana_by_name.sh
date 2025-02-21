#!/bin/bash

# Path to your Python script
PYTHON_SCRIPT="./crawl_grafana_by_name.py"

# nohup bash crawl_grafana_by_name.sh > ../log/crawl_grafana_by_name.log 2>&1 &

# Check if the Python script exists
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "Python script $PYTHON_SCRIPT not found."
    exit 1
fi

# Function to start the Python script
start_script() {
    /opt/miniconda3/envs/hub_exp/bin/python "$PYTHON_SCRIPT" &
    # Get the process ID of the Python script
    SCRIPT_PID=$!
    echo "Started Python script with PID $SCRIPT_PID"
}

# Function to stop the Python script gracefully
stop_script() {
    if [ -n "$SCRIPT_PID" ]; then
        echo "Stopping Python script with PID $SCRIPT_PID..."
        kill "$SCRIPT_PID"
        wait "$SCRIPT_PID" 2>/dev/null
    fi
    exit 0
}

# Trap signals to stop the script gracefully
trap stop_script SIGTERM SIGINT

# Start the script for the first time
start_script

while true; do
    # Check if the Python script is still running
    if ! ps -p "$SCRIPT_PID" > /dev/null; then
        echo "Python script has stopped. Restarting..."
        start_script
    fi
    # Check every 10 seconds
    sleep 10
done