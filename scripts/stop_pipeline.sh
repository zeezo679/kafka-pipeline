#!/usr/bin/env bash

PID_DIR="./.pids"
PRODUCER_PID_FILE="$PID_DIR/producer.pid"
SPARK_PID_FILE="$PID_DIR/spark_streaming.pid"

stop_process() {
  local name=$1
  local pid_file=$2

  if [ -f "$pid_file" ]; then
    local pid
    pid=$(cat "$pid_file")

    if ps -p "$pid" > /dev/null 2>&1; then
      echo "Stopping $name (PID: $pid) gracefully..."
      
      # Send SIGINT (Ctrl+C equivalent) first for safe stream termination
      kill -2 "$pid" 2>/dev/null || kill -15 "$pid" 2>/dev/null
      
      # Wait up to 15 seconds for clean exit
      local count=0
      while ps -p "$pid" > /dev/null 2>&1 && [ "$count" -lt 15 ]; do
        sleep 1
        count=$((count + 1))
      done

      # Force kill if process is still stuck
      if ps -p "$pid" > /dev/null 2>&1; then
        echo "Process $pid did not exit in time. Force killing..."
        kill -9 "$pid" 2>/dev/null
      fi

      echo "✔ $name stopped."
    else
      echo "Process $name (PID: $pid) is not running."
    fi
    rm -f "$pid_file"
  else
    echo "No PID file found for $name."
  fi
}

echo "=== Terminating Streaming Pipeline ==="
stop_process "PySpark Streaming" "$SPARK_PID_FILE"
stop_process "Python Kafka Producer" "$PRODUCER_PID_FILE"

echo ""
echo "=================================================="
echo "Pipeline stopped successfully."
echo "HDFS Parquet files, Spark checkpoints, and Kafka offsets are intact."
echo "=================================================="
