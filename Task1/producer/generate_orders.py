"""
Continuously simulates live order transactions and publishes them to Kafka.

Run standalone:  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python generate_orders.py
Run in compose:  the `producer` service already sets the right env vars.
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") #entry point to establish initial connection with the running broker
TOPIC = os.getenv("KAFKA_TOPIC", "topic1_logs")
MIN_INTERVAL = float(os.getenv("MIN_INTERVAL", "1.0"))
MAX_INTERVAL = float(os.getenv("MAX_INTERVAL", "2.0"))


#asynchronous
def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        acks=1,         
        linger_ms=50,     
        retries=5,
    )


def generate_order(order_id: int) -> dict:
    return {
        "order_id": order_id,
        "customer_id": random.randint(100, 500),
        "product_id": random.randint(1, 50),
        "quantity": random.randint(1, 10),
        "price": round(random.uniform(5.00, 500.00), 2),
        "order_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }


def on_send_success(record_metadata):
    print(
        f"  delivered -> partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_send_error(exc):
    print(f"  delivery FAILED: {exc}")


def main():
    producer = build_producer()
    order_id = 1001
    print(f"producing to '{TOPIC}' @ {BOOTSTRAP_SERVERS} "
          f"(interval {MIN_INTERVAL}-{MAX_INTERVAL}s). Ctrl+C to stop.")

    try:
        while True:
            order = generate_order(order_id)
            # key = order_id -> same order_id always lands on the same partition,
            # but does NOT give you per-customer ordering (see notes on partitioning).
            promise = producer.send(TOPIC, key=order_id, value=order)
            promise.add_callback(on_send_success).add_errback(on_send_error)

            print(f"queued    -> {order}")
            order_id += 1
            time.sleep(random.uniform(MIN_INTERVAL, MAX_INTERVAL))
    except KeyboardInterrupt:
        print("\nstopping producer...")
    finally:
        producer.flush(timeout=10)
        producer.close()


if __name__ == "__main__":
    main()