# kafka-pipeline

An end-to-end streaming data platform: a synthetic order-event producer on
Kafka, a Spark Structured Streaming job that lands enriched records in HDFS
as Parquet, and a Hive/Hue layer for OLAP-style querying on top — all
running as a single Dockerized cluster.

```
Kafka Producer  →  Kafka (topic1_logs)  →  Spark Structured Streaming
                                                      │
                                                      ▼
                                     HDFS (Parquet, checkpointed)
                                                      │
                                                      ▼
                                Hive external table (ecommerce_dw.streaming_orders)
                                                      │
                                                      ▼
                                              Hue (SQL editor)
```

## Repository layout

```
kafka-pipeline/
├── data-platform/              # the Docker cluster — everything in this README's
│   │                           # "Setup" section runs from inside this directory
│   ├── docker-compose.yml
│   ├── hive/
│   │   ├── Dockerfile          # apache/hive:4.0.0 + Postgres JDBC driver
│   │   └── conf/
│   │       ├── core-site.xml   # fs.defaultFS → namenode:9000
│   │       └── hdfs-site.xml
│   └── hue/
│       └── hue.ini             # points Hue's SQL editor at hive-server:10000
├── Task1/
│   └── producer/
│       └── generate_orders.py  # synthetic order-event Kafka producer
├── Task2/
│   └── spark/
│       └── spark_streaming.py  # Kafka → HDFS Structured Streaming job
├── scripts/
│   ├── start_pipeline.sh       # brings up HDFS/Kafka checks, Hive DDL,
│   │                           # producer, and Spark job as one command
│   └── stop_pipeline.sh        # gracefully stops producer + Spark job
└── README.md
```

## Architecture

| Layer | Component | Role |
|---|---|---|
| Ingestion | Kafka (`confluentinc/cp-kafka:7.5.0`, KRaft mode, single broker) | Receives synthetic order events on `topic1_logs` |
| Processing | Spark Structured Streaming (`spark-sql-kafka-0-10`) | Reads from Kafka, parses JSON, enriches with `total_amount`, writes Parquet with a 10s micro-batch trigger |
| Storage | HDFS (`bde2020` namenode/datanode, Hadoop 3.2.1) | Durable store for both the Parquet output and Spark's checkpoint state |
| Metadata / SQL | Hive 4.0.0 (`apache/hive`), split into `hive-metastore` + `hive-server`, Postgres-backed metastore | Exposes the HDFS Parquet data as a queryable external table |
| Query UI | Hue (`gethue/hue:latest`) | Browser-based SQL editor against HiveServer2 |

No ZooKeeper — Kafka runs in KRaft (combined broker+controller) mode.
No embedded Derby — Hive's metastore is backed by a dedicated Postgres
container so metadata survives independently of any one Hive container.

## Prerequisites

- Docker and Docker Compose v2
- Python 3.9+ with the `kafka-python` package, for running the producer outside a container
- (Optional, for `Task2`) a local Spark install if you want to `spark-submit` the streaming job from your host rather than a container
- ~6 GB of free RAM for the full stack (Kafka + HDFS x2 + Postgres + Hive x2 + Hue)

## Setup

Everything below runs from the `data-platform/` directory.

```bash
cd data-platform
docker compose up -d --build
```

This brings up, in dependency order: `postgres-metastore` → `namenode`/`datanode`
→ `hive-metastore` (runs `schematool` against Postgres automatically on first
boot) → `hive-server` → `hue`, plus `kafka` independently.

First boot takes a minute or two — Hive's schema init and HDFS leaving safe
mode both need to complete before the upper layers are usable. Verify
bottom-up rather than jumping straight to Hue:

1. **HDFS:** `http://localhost:9870` — NameNode overview page, 1 live datanode.
2. **Metastore:** `docker compose logs hive-metastore | tail -30` — look for
   `Starting Hive Metastore Server` with no exceptions.
3. **HiveServer2:**
   ```bash
   docker exec -it hive-server beeline -u 'jdbc:hive2://localhost:10000/'
   ```
4. **Hue:** `http://localhost:8888`
5. **Kafka:**
   ```bash
   docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
   ```

## Running the pipeline

### Manually (one component at a time)

```bash
# Terminal 1 — producer
python3 Task1/producer/generate_orders.py

# Terminal 2 — Spark Structured Streaming job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  Task2/spark/spark_streaming.py
```

### Automated lifecycle scripts

```bash
./scripts/start_pipeline.sh
```

This script, in order:
1. Waits for HDFS to report healthy (`hdfs dfsadmin -report`)
2. Waits for Kafka to respond, then creates `topic1_logs` if it doesn't already exist
3. Creates the required HDFS directories for the warehouse table and Spark checkpoint
4. Runs the Hive DDL to create `ecommerce_dw.streaming_orders` as an external Parquet table
5. Launches the producer in the background, logging to `logs/producer.log`
6. Launches the Spark Structured Streaming job in the background, logging to `logs/spark_streaming.log`

Stop everything cleanly with:

```bash
./scripts/stop_pipeline.sh
```

This sends `SIGINT` first (clean stream shutdown, flushing Kafka producer
buffers and committing Spark's checkpoint) and only force-kills after a
15-second grace period. HDFS Parquet files, Spark checkpoints, and Kafka
offsets are left intact — restarting `start_pipeline.sh` resumes from
where it left off rather than reprocessing from scratch.

## Querying the data

```sql
USE ecommerce_dw;

SELECT COUNT(order_id) AS processed_orders FROM streaming_orders;
SELECT SUM(total_amount) AS gross_revenue FROM streaming_orders;
SELECT AVG(total_amount) AS aov FROM streaming_orders;

SELECT product_id, SUM(total_amount) AS total_revenue
FROM streaming_orders
GROUP BY product_id
ORDER BY total_revenue DESC;

SELECT customer_id, COUNT(order_id) AS total_orders, SUM(total_amount) AS total_expenditure
FROM streaming_orders
GROUP BY customer_id
ORDER BY total_expenditure DESC;
```

Run these via Hue's SQL editor, or directly:

```bash
docker exec -it hive-server beeline -u 'jdbc:hive2://localhost:10000/'
```

## Known issues

- **`order_id` starts at 1013, not 1001, in Hive/Hue query results.** Under
  investigation — not yet root-caused.
- **Duplicate rows after a stop → wait → restart cycle** where the producer
  is restarted before the Spark job resumes consuming. A clean
  stop/immediate-restart cycle does not reproduce this; it only appears
  after a gap. Not yet fixed — checkpoint-based recovery works correctly
  in the straightforward restart case, but this edge case needs more
  investigation into how the Kafka consumer offsets and Spark checkpoint
  interact across a delayed resume.

## Notes on version choices

- **Hadoop is 3.2.1** (`bde2020` images), not the newer 3.4.0 — that
  maintainer never published a 3.4.0 tag, and the official `apache/hadoop`
  image requires hand-writing namenode/datanode formatting and startup
  logic rather than the env-var-driven config `bde2020`'s images provide.
- **Hive is genuinely 4.0.0** (official `apache/hive` image). It doesn't
  ship a Postgres JDBC driver by default, hence the custom
  `data-platform/hive/Dockerfile`, which adds it via `COPY --chown` rather
  than `ADD <url>` (Docker's `ADD` forces root-owned, `0600` permissions on
  remotely-fetched files, which the non-root `hive` user in this image
  can't read — a known packaging issue, HIVE-27955).
