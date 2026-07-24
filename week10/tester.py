"""Week 10 capstone — the first consumer that REACTS to your build.

On each ImagePushed it deploys the image (docker pull + run), runs an acceptance
test against the running container, and announces the outcome as a NEW event
(TestsPassed / TestsFailed). It never calls back to Jenkins — the pipeline stated
a fact and moved on, and the tester states a fact in turn.

Hardened with this week's patterns:
  idempotency  an image:tag already tested is skipped (redelivery is a no-op)
  retry        the container is not up the instant it starts -> retry with backoff
  dead-letter  an image that never becomes testable -> ci.images.dlq, do not block
"""
import json
import os
import subprocess
import time
import urllib.request

from kafka import KafkaConsumer, KafkaProducer

BROKER = "localhost:9092"
IN, OUT, DLQ = "ci.images", "ci.tests", "ci.images.dlq"
REGISTRY = "localhost:5001"
HOST_PORT = 18080
TESTED = ".tested_tags"


def already_tested():
    if not os.path.exists(TESTED):
        return set()
    return set(open(TESTED).read().split())


def acceptance_test():
    """The acceptance criterion: 1 + 2 must equal 3. Raises if the service is not
    reachable yet (a retryable condition); returns True/False once it answers."""
    url = f"http://localhost:{HOST_PORT}/sum?a=1&b=2"
    got = urllib.request.urlopen(url, timeout=3).read().decode().strip()
    print(f"    GET /sum?a=1&b=2 -> {got!r} (want '3')")
    return got == "3"


def deploy_and_test(image, tag):
    """Deploy the image and test it. Returns True (passed) / False (ran, wrong
    answer), or None if it never became testable (pull/run failed, or never
    answered) -> a dead-letter case."""
    ref = f"{REGISTRY}/{image}:{tag}"
    name = f"acctest-{image}-{tag}"
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    try:
        subprocess.run(["docker", "pull", ref], check=True, capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p", f"{HOST_PORT}:5000", ref],
                       check=True, capture_output=True)
    except subprocess.CalledProcessError as err:
        print(f"    deploy failed: {err.stderr.decode().strip()[:120]}")
        return None
    try:
        for attempt in range(1, 6):                  # retry — the service is not up instantly
            try:
                return acceptance_test()
            except OSError:                          # refused / reset / timeout: not ready yet
                wait = 2 ** (attempt - 1)
                print(f"    not up yet (attempt {attempt}); backoff {wait}s")
                time.sleep(wait)
        return None                                  # never answered -> dead-letter
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


tested = already_tested()
producer = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda v: json.dumps(v).encode())
consumer = KafkaConsumer(
    IN, bootstrap_servers=BROKER, group_id="tester",
    auto_offset_reset="earliest", enable_auto_commit=False,
    value_deserializer=lambda b: json.loads(b.decode()),
)

print("tester up — reacting to ImagePushed. Ctrl-C to stop")
for msg in consumer:
    event = msg.value
    image, tag = event.get("image"), str(event.get("tag"))
    key = f"{image}:{tag}"

    if key in tested:                                # idempotency
        print(f"[skip] {key} already tested")
        consumer.commit()
        continue

    print(f"[event] ImagePushed {key} -> deploying and testing")
    result = deploy_and_test(image, tag)

    if result is None:                               # dead-letter
        producer.send(DLQ, {"original": event, "reason": "image never became testable"}).get(timeout=10)
        print(f"[DLQ] {key} -> {DLQ}")
        consumer.commit()
        continue

    outcome = "TestsPassed" if result else "TestsFailed"
    producer.send(OUT, {"event": outcome, "image": image, "tag": tag}).get(timeout=10)
    print(f"[{outcome}] {key} announced on {OUT}")
    tested.add(key)
    open(TESTED, "a").write(key + "\n")
    consumer.commit()
