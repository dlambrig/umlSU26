"""Week 8 lab — publish a stream of events to Kafka, then exit.

The point of the lab: this program does NOT wait for a consumer. It hands each
event to the broker and stops. The broker holds the events until some consumer
shows up to read them — that is the essence of event-driven decoupling.
"""
import json
import time

from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "orders"

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Several OrderPlaced events. The producer publishes them one after another,
# pausing briefly between each, and never checks whether anyone is listening.
events = [
    {"event": "OrderPlaced", "order_id": 42, "amount": 19.99},
    {"event": "OrderPlaced", "order_id": 43, "amount": 5.50},
    {"event": "OrderPlaced", "order_id": 44, "amount": 120.00},
]

for event in events:
    # send() is asynchronous; .get() blocks just long enough to confirm the write.
    metadata = producer.send(TOPIC, event).get(timeout=10)
    print(
        f"produced {event}\n"
        f"  -> topic '{metadata.topic}', partition {metadata.partition}, offset {metadata.offset}"
    )
    time.sleep(1)  # a brief pause so you can watch them go out one at a time

producer.flush()
producer.close()
print("producer exited (it never waited for a consumer).")
