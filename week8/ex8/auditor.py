"""Exercise 8 starter — the AUDITOR consumer.

Records every event, no matter its type. You do not need to edit this file.
It exists to show fan-out: the auditor and the reactor each get their OWN copy
of every event, and neither knows the other exists.
"""
import json
from kafka import KafkaConsumer

BROKER = "localhost:9092"
TOPIC = "events"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    group_id="auditor",               # its own group -> receives every event
    auto_offset_reset="earliest",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)
print("auditor: recording every event (Ctrl-C to stop)")
for message in consumer:
    print(f"auditor  logged: {message.value}")
