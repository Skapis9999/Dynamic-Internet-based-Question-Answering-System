"""HTTP front door: POST a question, stream progress + answers back over Server-Sent Events."""
import asyncio
import json
import threading
import time
import uuid
from collections import defaultdict, deque
from pathlib import Path

from confluent_kafka import Consumer
from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from services.common import bus, config

log = bus.setup_logging("api")
app = FastAPI(title="Dynamic Internet-based QA")
producer = bus.make_producer()

# qid -> recent events (so a browser that connects slightly late misses nothing)
history: dict[str, deque] = defaultdict(lambda: deque(maxlen=200))
subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
loop: asyncio.AbstractEventLoop | None = None


def _dispatch(event: dict) -> None:
    qid = event["qid"]
    history[qid].append(event)
    for q in subscribers.get(qid, ()):
        q.put_nowait(event)


def _consume_events() -> None:
    # Unique group per API pod: every replica sees every event.
    c: Consumer = bus.make_consumer(f"api-{uuid.uuid4().hex[:8]}", [config.T_EVENTS], offset_reset="latest")
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        loop.call_soon_threadsafe(_dispatch, json.loads(msg.value()))


@app.on_event("startup")
async def startup() -> None:
    global loop
    loop = asyncio.get_running_loop()
    threading.Thread(target=_consume_events, daemon=True).start()


class Ask(BaseModel):
    question: str


@app.post("/api/ask")
def ask(body: Ask) -> dict:
    qid = uuid.uuid4().hex[:12]
    bus.send(producer, config.T_QUESTIONS, qid, {"qid": qid, "question": body.question.strip()})
    bus.emit_event(producer, qid, "accepted", question=body.question.strip())
    producer.flush()
    return {"qid": qid}


@app.get("/api/stream/{qid}")
async def stream(qid: str) -> StreamingResponse:
    queue: asyncio.Queue = asyncio.Queue()
    subscribers[qid].add(queue)

    async def gen():
        try:
            for ev in list(history.get(qid, ())):
                yield f"data: {json.dumps(ev)}\n\n"
            deadline = time.time() + 300
            while time.time() < deadline:
                try:
                    ev = await asyncio.wait_for(queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
                yield f"data: {json.dumps(ev)}\n\n"
                if ev["type"] == "final":
                    break
        finally:
            subscribers[qid].discard(queue)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True}


@app.get("/")
def index() -> FileResponse:
    return FileResponse(Path(__file__).parent / "static" / "index.html")
