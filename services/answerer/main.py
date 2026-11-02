"""Stage 3: retrieve the best chunks for a question and let the LLM answer with citations.

Like the thesis' stage2/stage3 split, it answers twice:
  * "fast"  - as soon as the first page with text is indexed,
  * "final" - when every page is processed, or ANSWER_DEADLINE_S has passed.
"""
import time
from dataclasses import dataclass, field

from qdrant_client import QdrantClient, models

from services.common import bus, config, ollama

log = bus.setup_logging("answerer")
qdrant = QdrantClient(url=config.QDRANT_URL)

SYSTEM = (
    "You answer questions using ONLY the numbered web sources provided. "
    "Be concise and factual. Cite sources inline like [1] or [2][3], using ONLY the source "
    "numbers given in the headers; never copy reference numbers that appear inside source text. "
    "If the sources do not contain the answer, say so plainly instead of guessing."
)


@dataclass
class Q:
    question: str
    expected: int
    started: float = field(default_factory=time.time)
    done: int = 0
    with_text: int = 0
    fast_sent: bool = False


state: dict[str, Q] = {}
finished: set[str] = set()  # late pages arriving after the final answer are ignored


def retrieve(qid: str, question: str) -> list[dict]:
    qvec = ollama.embed([question])[0]
    hits = qdrant.query_points(
        config.QDRANT_COLLECTION, query=qvec, limit=config.TOP_K, with_payload=True,
        query_filter=models.Filter(must=[models.FieldCondition(key="qid", match=models.MatchValue(value=qid))]),
    ).points
    return [{**h.payload, "score": h.score} for h in hits]


def answer(qid: str, q: Q, kind: str) -> None:
    t0 = time.time()
    hits = retrieve(qid, q.question)
    if not hits:
        bus.emit_event(producer, qid, kind, answer="No usable text was found on the retrieved pages.", sources=[])
        return

    # Number sources per distinct URL so citations map to pages, not chunks.
    sources, num = [], {}
    for h in hits:
        if h["url"] not in num:
            num[h["url"]] = len(num) + 1
            sources.append({"n": num[h["url"]], "url": h["url"], "title": h.get("title", "")})
    context = "\n\n".join(f"[{num[h['url']]}] ({h['url']})\n{h['text']}" for h in hits)

    text = ollama.chat([
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": f"Sources:\n{context}\n\nQuestion: {q.question}"},
    ])
    bus.emit_event(producer, qid, kind, answer=text.strip(), sources=sources,
                   pages_used=q.done, pages_expected=q.expected, llm_seconds=round(time.time() - t0, 1),
                   total_seconds=round(time.time() - q.started, 1))
    producer.flush()
    log.info("[%s] %s answer in %.1fs (pages %d/%d)", qid, kind, time.time() - t0, q.done, q.expected)


def handle(_topic: str, msg: dict) -> None:
    if msg["qid"] in finished:
        return
    q = state.setdefault(msg["qid"], Q(question=msg["question"], expected=msg["expected"]))
    q.done += 1
    if msg.get("n_chunks"):
        q.with_text += 1


def tick() -> None:
    now = time.time()
    for qid, q in list(state.items()):
        if q.done >= q.expected or now - q.started > config.ANSWER_DEADLINE_S:
            del state[qid]
            finished.add(qid)
            answer(qid, q, "final")
        elif q.with_text and not q.fast_sent:
            q.fast_sent = True
            answer(qid, q, "fast")


if __name__ == "__main__":
    producer = bus.make_producer()
    bus.run_loop(bus.make_consumer("answerer", [config.T_CHUNKS_READY]), handle, idle=tick)
