"""Stage 3: retrieve the best chunks for a question and let the LLM answer with citations.

Like the thesis' stage2/stage3 split, it answers twice:
  * "fast"  - once FAST_MIN_PAGES pages with text are indexed,
  * "final" - when every page is processed, or ANSWER_DEADLINE_S has passed.
Answers are streamed to the UI token by token ("delta" events), then sent complete.
"""
import time
from dataclasses import dataclass, field
from datetime import date

from qdrant_client import QdrantClient, models

from services.common import bus, config, ollama

log = bus.setup_logging("answerer")
qdrant = QdrantClient(url=config.QDRANT_URL)

SYSTEM = """You answer questions using ONLY the numbered web sources provided. Today's date is {today}.
Rules:
- Be concise and factual. Cite sources inline like [1] or [2][3], using ONLY the source numbers given
  in the headers; never copy reference numbers that appear inside source text.
- If the sources describe DIFFERENT things that match the question (e.g. two places with the same
  name), say the question is ambiguous and answer briefly for each.
- For time-sensitive facts (current office holders, latest events), prefer the most recent information
  and mention its date if sources disagree.
- If the sources do not contain the answer, say so plainly instead of guessing."""

DELTA_EVERY_S = 0.25


@dataclass
class Q:
    question: str
    expected: int
    asked_at: float
    done: set[str] = field(default_factory=set)  # pages reported (a retried Spark task may report twice)
    urls: list[str] = field(default_factory=list)  # pages that produced text
    fast_sent: bool = False


state: dict[str, Q] = {}
finished: set[str] = set()  # late pages arriving after the final answer are ignored


def retrieve(q: Q) -> list[dict]:
    """Top chunks among this question's pages, at most MAX_CHUNKS_PER_SOURCE per page."""
    qvec = ollama.embed([q.question])[0]
    hits = qdrant.query_points(
        config.QDRANT_COLLECTION, query=qvec, limit=config.TOP_K * 3, with_payload=True,
        query_filter=models.Filter(must=[models.FieldCondition(key="url", match=models.MatchAny(any=q.urls))]),
    ).points
    picked, per_url = [], {}
    for h in hits:
        u = h.payload["url"]
        if per_url.get(u, 0) < config.MAX_CHUNKS_PER_SOURCE:
            per_url[u] = per_url.get(u, 0) + 1
            picked.append({**h.payload, "score": h.score})
        if len(picked) == config.TOP_K:
            break
    return picked


def answer(qid: str, q: Q, kind: str) -> None:
    t0 = time.time()
    hits = retrieve(q) if q.urls else []
    if not hits:
        msg = ("No usable text was found on the retrieved pages."
               if len(q.done) >= q.expected else
               f"I couldn't read the web pages in time ({len(q.done)} of {q.expected} processed). "
               "The system may be busy or still starting up, so please try again.")
        bus.emit_event(producer, qid, kind, answer=msg, sources=[],
                       pages_used=len(q.done), pages_expected=q.expected,
                       total_seconds=round(time.time() - q.asked_at, 1))
        producer.flush()
        return

    # Number sources per distinct URL so citations map to pages, not chunks.
    sources, num = [], {}
    for h in hits:
        if h["url"] not in num:
            num[h["url"]] = len(num) + 1
            sources.append({"n": num[h["url"]], "url": h["url"], "title": h.get("title", "")})
    context = "\n\n".join(f"[{num[h['url']]}] ({h['url']})\n{h['text']}" for h in hits)
    messages = [
        {"role": "system", "content": SYSTEM.format(today=date.today().isoformat())},
        {"role": "user", "content": f"Sources:\n{context}\n\nQuestion: {q.question}"},
    ]

    text, pending, last = "", "", time.time()
    first_token = None
    for delta in ollama.chat_stream(messages):
        first_token = first_token or time.time()
        text += delta
        pending += delta
        if time.time() - last >= DELTA_EVERY_S:
            bus.emit_event(producer, qid, "delta", kind=kind, text=pending)
            pending, last = "", time.time()
    if pending:
        bus.emit_event(producer, qid, "delta", kind=kind, text=pending)

    bus.emit_event(producer, qid, kind, answer=text.strip(), sources=sources,
                   pages_used=len(q.done), pages_expected=q.expected,
                   llm_seconds=round(time.time() - t0, 1),
                   first_token_seconds=round((first_token or time.time()) - q.asked_at, 1),
                   total_seconds=round(time.time() - q.asked_at, 1))
    producer.flush()
    log.info("[%s] %s answer in %.1fs (pages %d/%d)", qid, kind, time.time() - t0, len(q.done), q.expected)


def handle(_topic: str, msg: dict) -> None:
    if msg["qid"] in finished:
        return
    q = state.setdefault(msg["qid"], Q(question=msg["question"], expected=msg["expected"],
                                       asked_at=msg.get("asked_at") or time.time()))
    if msg.get("type") == "started" or msg["url"] in q.done:
        return
    q.done.add(msg["url"])
    if msg.get("n_chunks"):
        q.urls.append(msg["url"])


def tick() -> None:
    now = time.time()
    for qid, q in list(state.items()):
        if len(q.done) >= q.expected or now - q.asked_at > config.ANSWER_DEADLINE_S:
            del state[qid]
            finished.add(qid)
            try:
                answer(qid, q, "final")
            except Exception as e:  # e.g. Ollama down: the question still needs a final answer
                log.exception("[%s] final answer failed", qid)
                bus.emit_event(producer, qid, "final", sources=[],
                               answer=f"The language model failed to answer ({type(e).__name__}). Please try again.",
                               total_seconds=round(time.time() - q.asked_at, 1))
                producer.flush(5)
        elif not q.fast_sent and len(q.urls) >= config.FAST_MIN_PAGES:
            q.fast_sent = True
            answer(qid, q, "fast")


if __name__ == "__main__":
    producer = bus.make_producer()
    bus.run_loop(bus.make_consumer("answerer", [config.T_CHUNKS_READY]), handle, idle=tick)
