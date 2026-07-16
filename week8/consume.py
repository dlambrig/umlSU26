"""Week 8 lab — Part A: subscribe to the topic and print what arrives.

Run this AFTER produce.py has already exited to see the decoupling: the event
was held by the broker and is delivered to you now. `auto_offset_reset="earliest"`
means a brand-new consumer group reads the topic from the beginning, so you see
messages that were produced before you started.

Ctrl-C to stop.
"""
import json
from kafka import KafkaConsumer

BROKER = "localhost:9092"
TOPIC = "orders"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    group_id="week8-demo",
    auto_offset_reset="earliest",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

print(f"consuming from '{TOPIC}' (Ctrl-C to stop)...")
try:
    for message in consumer:
        print(
            f"consumed {message.value}"
            f"  (partition {message.partition}, offset {message.offset})"
        )
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
