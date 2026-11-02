"""Stage 2: URL -> HTML -> clean text -> chunks -> embeddings -> Qdrant.

Plain-Python version (Phase 0). Phase 1 replaces this with Spark Structured Streaming,
reusing the pure functions in services/fetcher/pipeline.py.
"""
from services.common import bus, config
from services.fetcher import pipeline

log = bus.setup_logging("fetcher")


def handle(_topic: str, msg: dict) -> None:
    qid, url = msg["qid"], msg["url"]
    n, err = 0, None
    try:
        text, title = pipeline.fetch_and_extract(url)
        chunks = pipeline.chunk(text)
        n = pipeline.embed_and_store(qid, url, title or msg.get("title", ""), msg.get("rank", 0), chunks)
        log.info("[%s] %s -> %d chunks", qid, url, n)
    except Exception as e:
        err = f"{type(e).__name__}: {e}"[:300]
        log.warning("[%s] %s failed: %s", qid, url, err)

    bus.emit_event(producer, qid, "page", url=url, n_chunks=n, error=err)
    # Always report back (even on failure) so the answerer can count pages done.
    bus.send(producer, config.T_CHUNKS_READY, qid, {
        "qid": qid, "question": msg["question"], "url": url,
        "n_chunks": n, "expected": msg["expected"], "error": err,
    })
    producer.flush()


if __name__ == "__main__":
    pipeline.ensure_collection()
    producer = bus.make_producer()
    bus.run_loop(bus.make_consumer("fetcher", [config.T_URLS]), handle)
