# Week 8 Lab — Why Event-Driven Systems?

Starter files for the Week 8 labs. You **edit and run** these; you don't write them
from scratch. Two labs this week: a Kafka event walkthrough, and running your own
container registry. (Exercise 8 builds on the Kafka lab — its starters are in `ex8/`.)

## Prerequisites
- Docker Desktop (or Docker Engine) running.
- Python 3.11+ with a virtual environment (for the Kafka client).

---

## Lab 1 — Your first Kafka event

Bring up a single-broker Kafka (KRaft mode, no ZooKeeper):

```bash
cd week8
docker compose up -d          # starts one apache/kafka broker
docker compose ps             # confirm it's running
```

Install the Python client and publish a stream of events:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python produce.py             # publishes several 'OrderPlaced' events to 'orders', then exits
```

In another terminal, read them back:

```bash
source .venv/bin/activate
python consume.py             # subscribes to 'orders', prints what arrives (Ctrl-C to stop)
```

You'll see the events `produce.py` sent — even though the producer already exited.
The broker held them until the consumer showed up. Producer and consumer never knew
about each other; they only share the topic name `orders`.

Tear down when done:

```bash
docker compose down
```

> **Client note:** the notes mention `kafka-python`. On modern Python (3.12+) use
> `kafka-python-ng` (pinned in `requirements.txt`) — it's the maintained drop-in
> fork, still imported as `import kafka`. The concepts are identical.

---

## Lab 2 — Run your own registry (optional)

No starter files — the whole lab is command-line. See the Week 8 notes for the
walkthrough. In brief:

```bash
docker run -d -p 5001:5000 --name registry registry:2   # port 5001: macOS uses 5000
docker pull alpine:3.19
docker tag alpine:3.19 localhost:5001/demo/hello:v1
docker push localhost:5001/demo/hello:v1
curl localhost:5001/v2/_catalog
curl localhost:5001/v2/demo/hello/tags/list
docker rm -f registry                                    # tear down
```

A registry is a versioned store for container images — where a CI pipeline
publishes what it builds so deployments can pull the exact same image later.

---

## Files
| File | Purpose |
|------|---------|
| `docker-compose.yml` | single-broker Kafka (KRaft) |
| `requirements.txt` | Kafka Python client (`kafka-python-ng`) |
| `produce.py` | publish several `OrderPlaced` events to `orders`, then exit |
| `consume.py` | subscribe to `orders` and print events |
| `ex8/producer_stream.py` | Exercise 8 starter — publish a stream of your own events |
| `ex8/auditor.py` | Exercise 8 starter — consumer that records every event |
| `ex8/reactor.py` | Exercise 8 starter — consumer that reacts by event type |

> The Kafka image tag (`apache/kafka:3.9.0`) is pinned for reproducibility.
> **Validated end-to-end** on macOS (Docker + Python 3.14): produce → consume
> delivers the held events, and the registry lab pushes/lists/pulls locally.
