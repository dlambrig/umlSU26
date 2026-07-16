"""Exercise 8 starter — the REACTOR consumer.

This one reacts DIFFERENTLY depending on the event type. You only edit the
handle() function: add one branch per event type you care about (Part 1).
Everything else can stay as-is.
"""
import json
from kafka import KafkaConsumer

BROKER = "localhost:9092"
TOPIC = "events"

running_total = 0.0


def handle(event):
    global running_total
    # ----- EDIT: one branch per event type you want to react to -------------
    if event.get("type") == "OrderPlaced":
        running_total += event.get("amount", 0)
        print(f"reactor: order placed, running total = {running_total:.2f}")
    elif event.get("type") == "OrderCancelled":
        print(f"reactor: order {event.get('order_id')} cancelled (total unchanged)")
    else:
        print(f"reactor: ignoring {event.get('type')}")
    # ------------------------------------------------------------------------


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    group_id="reactor",               # its own group -> also receives every event
    auto_offset_reset="earliest",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)
print("reactor: reacting to events (Ctrl-C to stop)")
for message in consumer:
    handle(message.value)
