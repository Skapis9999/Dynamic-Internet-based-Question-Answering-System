"""Stage 2 (plain-Python engine): URL -> HTML -> clean text -> chunks -> embeddings -> Qdrant.

Kept for comparison with the Spark engine (services/spark_fetcher/job.py); both use pipeline.py.
"""
import time

from services.common import bus, config
from services.fetcher import pipeline

log = bus.setup_logging("fetcher")


def handle(_topic: str, msg: dict) -> None:
    qid, url = msg["qid"], msg["url"]
    res = pipeline.process_url(url, msg.get("title", ""))
    log.info("[%s] %s -> %s", qid, url, res)
    bus.emit_event(producer, qid, "page", url=url, engine="python", **res)
    # Always report back (even on failure) so the answerer can count pages done.
    bus.send(producer, config.T_CHUNKS_READY, qid, {
        "qid": qid, "question": msg["question"], "url": url, "expected": msg["expected"],
        "asked_at": msg.get("asked_at"), "n_chunks": res["n_chunks"], "error": res["error"],
    })
    producer.flush()


if __name__ == "__main__":
    for attempt in range(30):  # DNS/Qdrant may not be up yet after a cluster restart
        try:
            pipeline.ensure_collection()
            break
        except Exception as e:
            log.warning("qdrant not ready (%s), retrying...", e)
            time.sleep(2)
    else:
        raise SystemExit("qdrant unreachable")
    producer = bus.make_producer()
    bus.run_loop(bus.make_consumer("fetcher", [config.T_URLS]), handle)
