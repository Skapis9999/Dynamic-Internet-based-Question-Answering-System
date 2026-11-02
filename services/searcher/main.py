"""Stage 1: question -> search queries (LLM) -> SearXNG -> one Kafka message per URL.

Successor of the thesis' findImportantWord + searchAuto, with the LLM writing the queries.
"""
import json
from urllib.parse import urldefrag, urlparse

import httpx

from services.common import bus, config, ollama

log = bus.setup_logging("searcher")
http = httpx.Client(timeout=20)

# Domains whose pages are useless as text sources.
SKIP_DOMAINS = ("youtube.com", "facebook.com", "instagram.com", "tiktok.com", "x.com", "twitter.com", "pinterest.")

QUERY_SCHEMA = {
    "type": "object",
    "properties": {"queries": {"type": "array", "items": {"type": "string"}}},
    "required": ["queries"],
}


def plan_queries(question: str) -> list[str]:
    try:
        out = ollama.chat([
            {"role": "system", "content": (
                f"You write web search queries. Given a question, return up to {config.NUM_QUERIES} short, "
                "diverse keyword queries that would find pages containing the answer. JSON only.")},
            {"role": "user", "content": question},
        ], fmt=QUERY_SCHEMA)
        queries = [q.strip() for q in json.loads(out)["queries"] if q.strip()]
    except Exception:
        log.exception("query planning failed, falling back to raw question")
        queries = []
    # Always include the raw question too: it's what the thesis searched with.
    return [question] + [q for q in queries if q.lower() != question.lower()][: config.NUM_QUERIES]


def search(query: str) -> list[dict]:
    r = http.get(f"{config.SEARXNG_URL}/search", params={"q": query, "format": "json", "language": "en"})
    r.raise_for_status()
    return r.json().get("results", [])


def handle(_topic: str, msg: dict) -> None:
    qid, question = msg["qid"], msg["question"]
    log.info("[%s] %s", qid, question)

    queries = plan_queries(question)
    bus.emit_event(producer, qid, "queries", queries=queries)

    # Round-robin merge across queries so every query contributes its best hits.
    per_query = []
    for q in queries:
        try:
            per_query.append(search(q))
        except Exception as e:
            log.warning("search failed for %r: %s", q, e)
    seen, picked = set(), []
    for rank in range(max((len(r) for r in per_query), default=0)):
        for results in per_query:
            if rank >= len(results) or len(picked) >= config.MAX_URLS:
                continue
            res = results[rank]
            url = urldefrag(res.get("url", ""))[0]
            host = urlparse(url).netloc
            if not url.startswith("http") or url in seen or any(d in host for d in SKIP_DOMAINS):
                continue
            seen.add(url)
            picked.append({"url": url, "title": res.get("title", "")})

    bus.emit_event(producer, qid, "urls", urls=picked)
    if not picked:
        bus.emit_event(producer, qid, "final", answer="I couldn't find any web pages for this question.", sources=[])
        return

    for rank, p in enumerate(picked):
        # Keyed by URL (not qid) so the pages of ONE question spread over all
        # partitions -> fetched in parallel by every fetcher replica.
        bus.send(producer, config.T_URLS, p["url"], {
            "qid": qid, "question": question, "url": p["url"], "title": p["title"],
            "rank": rank, "expected": len(picked),
        })
    producer.flush()


if __name__ == "__main__":
    producer = bus.make_producer()
    bus.run_loop(bus.make_consumer("searcher", [config.T_QUESTIONS]), handle)
