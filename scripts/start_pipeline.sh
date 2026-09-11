#!/usr/bin/env bash

set -euo pipefail


# Initialize Conda for non-interactive shell execution
CONDA_BASE=$(conda info --base 2>/dev/null || echo "$HOME/anaconda3")
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate dataenv

# Configuration
KAFKA_BROKER="localhost:9092"
KAFKA_TOPIC="topic1_logs"
HDFS_NAMENODE="hdfs://localhost:9000"
HIVE_SERVER_CONTAINER="hive-server"
NAMENODE_CONTAINER="namenode"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PRODUCER_SCRIPT="$PROJECT_ROOT/Task1/producer/generate_orders.py"
SPARK_SCRIPT="$PROJECT_ROOT/Task2/spark/spark_streaming.py"
SPARK_PKG="org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"

PID_DIR="./.pids"
LOG_DIR="./logs"
mkdir -p "$PID_DIR" "$LOG_DIR"

echo "=== [1/6] Validating Hadoop NameNode & DataNode Health ==="
until docker exec "$NAMENODE_CONTAINER" hdfs dfsadmin -report > /dev/null 2>&1; do
  echo "Waiting for HDFS NameNode and DataNode to be healthy..."
  sleep 3
done
echo "✔ HDFS service is healthy."

echo "=== [2/6] Verifying Kafka Broker Responsiveness & Topic Creation ==="
until docker exec kafka kafka-topics --bootstrap-server "$KAFKA_BROKER" --list > /dev/null 2>&1; do
  echo "Waiting for Kafka broker ($KAFKA_BROKER)..."
  sleep 3
done

# Ensure Kafka topic exists
docker exec kafka kafka-topics --bootstrap-server "$KAFKA_BROKER" \
  --create --if-not-exists \
  --topic "$KAFKA_TOPIC" \
  --partitions 1 \
  --replication-factor 1
echo "✔ Kafka broker is ready and topic '$KAFKA_TOPIC' is guaranteed."

echo "=== [3/6] Creating Prerequisite HDFS Directories ==="
HDFS_PATHS=(
  "/user/hive/warehouse/ecommerce_dw.db/streaming_orders"
  "/user/spark/checkpoints/streaming_orders"
)

for path in "${HDFS_PATHS[@]}"; do
  if docker exec "$NAMENODE_CONTAINER" hdfs dfs -test -d "$path" > /dev/null 2>&1; then
    echo "Directory $path already exists. Skipping."
  else
    echo "Creating directory $path..."
    docker exec "$NAMENODE_CONTAINER" hdfs dfs -mkdir -p "$path"
  fi
done
echo "✔ HDFS storage and checkpoint paths ready."

echo "=== [4/6] Executing Hive DDL ==="
HIVE_DDL="
CREATE DATABASE IF NOT EXISTS ecommerce_dw;
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_dw.streaming_orders (
    order_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10, 2),
    order_time TIMESTAMP,
    total_amount DECIMAL(12, 2)
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/ecommerce_dw.db/streaming_orders';
"
docker exec -i "$HIVE_SERVER_CONTAINER" beeline -u "jdbc:hive2://localhost:10000" -e "$HIVE_DDL"
echo "✔ Hive DDL executed successfully."

echo "=== [5/6] Launching Python Kafka Producer ==="
python3 "$PRODUCER_SCRIPT" > "$LOG_DIR/producer.log" 2>&1 &
PRODUCER_PID=$!
echo "$PRODUCER_PID" > "$PID_DIR/producer.pid"
echo "✔ Producer running (PID: $PRODUCER_PID). Logs: $LOG_DIR/producer.log"

echo "=== [6/6] Launching PySpark Structured Streaming Job ==="
spark-submit \
  --packages "$SPARK_PKG" \
  "$SPARK_SCRIPT" > "$LOG_DIR/spark_streaming.log" 2>&1 &
SPARK_PID=$!
echo "$SPARK_PID" > "$PID_DIR/spark_streaming.pid"
echo "✔ Spark Streaming job running (PID: $SPARK_PID). Logs: $LOG_DIR/spark_streaming.log"

echo ""
echo "=================================================="
echo "Pipeline successfully started in background!"
echo "Use './stop_pipeline.sh' to terminate."
echo "=================================================="
