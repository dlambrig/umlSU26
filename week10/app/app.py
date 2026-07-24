"""A tiny calculator service — the image the pipeline builds, and the thing the
capstone deploys and acceptance-tests. Real endpoint so there is something to test.

Replaces the Week 9 throwaway echo image."""
from flask import Flask, request

app = Flask(__name__)


@app.get("/sum")
def add():
    a = int(request.args.get("a", 0))
    b = int(request.args.get("b", 0))
    return str(a + b)


@app.get("/health")
def health():
    return "ok"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
