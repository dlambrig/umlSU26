"""Exercise 8 starter — publish a STREAM of your own domain events.

You do NOT need to write Python. Just edit the EVENTS list below to match the
domain you designed in Part 1 (give every event a "type"). Then run this while
your consumers are watching — or even before them; the broker holds the events.
"""
import json
import time
from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "events"                      # you may rename this

# ----- EDIT: your events (each is a fact that already happened) -------------
EVENTS = [
    {"type": "OrderPlaced",    "order_id": 1, "amount": 19.99},
    {"type": "OrderPlaced",    "order_id": 2, "amount": 5.00},
    {"type": "OrderCancelled", "order_id": 1},
    {"type": "OrderPlaced",    "order_id": 3, "amount": 42.00},
]
# ----------------------------------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
for event in EVENTS:
    producer.send(TOPIC, event)
    print("published", event)
    time.sleep(1)                     # brief pause so you can watch consumers react
producer.flush()
producer.close()
print("producer done — it never waited for a consumer.")
