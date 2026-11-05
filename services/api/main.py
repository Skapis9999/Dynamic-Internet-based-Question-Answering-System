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
STREAM_TIMEOUT_S = 120

# qid -> recent events (so a browser that connects slightly late misses nothing)
history: dict[str, deque] = defaultdict(lambda: deque(maxlen=600))
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
    bus.send(producer, config.T_QUESTIONS, qid,
             {"qid": qid, "question": body.question.strip(), "asked_at": time.time()})
    bus.emit_event(producer, qid, "accepted", question=body.question.strip())
    producer.flush(5)  # bounded: with Kafka down the request must not hang (messages stay queued)
    return {"qid": qid}


@app.get("/api/stream/{qid}")
async def stream(qid: str) -> StreamingResponse:
    queue: asyncio.Queue = asyncio.Queue()
    subscribers[qid].add(queue)

    async def gen():
        try:
            for ev in list(history.get(qid, ())):
                yield f"data: {json.dumps(ev)}\n\n"
                if ev["type"] == "final":
                    return
            # Safety net if a stage is down and no one else can answer: never leave the UI waiting.
            deadline = time.time() + STREAM_TIMEOUT_S
            while (left := deadline - time.time()) > 0:
                try:
                    ev = await asyncio.wait_for(queue.get(), timeout=min(15, left))
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
                yield f"data: {json.dumps(ev)}\n\n"
                if ev["type"] == "final":
                    return
            timeout_event = {"qid": qid, "type": "final", "ts": time.time(), "sources": [],
                             "answer": "The system didn't answer in time. It may be busy or "
                                       "still starting up, so please try again.",
                             "total_seconds": STREAM_TIMEOUT_S}
            yield f"data: {json.dumps(timeout_event)}\n\n"
        finally:
            subscribers[qid].discard(queue)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True}


@app.get("/")
def index() -> FileResponse:
    return FileResponse(Path(__file__).parent / "static" / "index.html")
