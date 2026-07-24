"""Week 10 capstone — a SECOND consumer, reacting to the tester's result (the
fan-out from Week 9: the tester does not know the promoter exists).

On TestsPassed it promotes the image to the released tag (docker tag :latest +
push). Promotion is a stateful action, so it is idempotent: the same tag is never
promoted twice, even if the event is redelivered.
"""
import json
import os
import subprocess

from kafka import KafkaConsumer

BROKER = "localhost:9092"
IN = "ci.tests"
REGISTRY = "localhost:5001"
PROMOTED = ".promoted"


def already_promoted():
    if not os.path.exists(PROMOTED):
        return set()
    return set(open(PROMOTED).read().split())


promoted = already_promoted()
consumer = KafkaConsumer(
    IN, bootstrap_servers=BROKER, group_id="promoter",
    auto_offset_reset="earliest",
    value_deserializer=lambda b: json.loads(b.decode()),
)

print("promoter up — reacting to TestsPassed. Ctrl-C to stop")
for msg in consumer:
    event = msg.value
    if event.get("event") != "TestsPassed":
        print(f"[ignore] {event.get('event')} for {event.get('image')}:{event.get('tag')}")
        continue

    image, tag = event["image"], str(event["tag"])
    key = f"{image}:{tag}"
    if key in promoted:                              # idempotency — don't re-promote
        print(f"[skip] {key} already promoted")
        continue

    src = f"{REGISTRY}/{image}:{tag}"
    dst = f"{REGISTRY}/{image}:latest"
    subprocess.run(["docker", "pull", src], check=True, capture_output=True)
    subprocess.run(["docker", "tag", src, dst], check=True)
    subprocess.run(["docker", "push", dst], check=True, capture_output=True)
    promoted.add(key)
    open(PROMOTED, "a").write(key + "\n")
    print(f"[PROMOTED] {key} -> {image}:latest pushed to {REGISTRY}")
