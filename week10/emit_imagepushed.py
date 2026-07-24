"""Announce an ImagePushed event — stands in for the Week 9 Jenkins pipeline's
final step, so you can exercise the Week 10 consumers without running Jenkins.

    python emit_imagepushed.py 1              # ImagePushed calculator:1
    python emit_imagepushed.py 2 calculator   # ImagePushed <image>:<tag>
"""
import json
import sys

from kafka import KafkaProducer

tag = sys.argv[1] if len(sys.argv) > 1 else "1"
image = sys.argv[2] if len(sys.argv) > 2 else "calculator"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode(),
)
event = {"event": "ImagePushed", "image": image, "tag": tag, "registry": "localhost:5001"}
producer.send("ci.images", event).get(timeout=10)
producer.flush()
print("emitted", event)
