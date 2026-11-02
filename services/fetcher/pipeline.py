"""Pure page-processing functions, shared by the plain fetcher and the Spark job.

Chunks are stored per URL (not per question), so a page fetched for one question is
reused by later questions while it is younger than CACHE_TTL_S.
"""
import re
import threading
import time
import uuid
from collections import defaultdict
from urllib.parse import urlparse

import httpx
import trafilatura
from qdrant_client import QdrantClient, models

from services.common import config, ollama

# Honest bot identification: Wikimedia (and others) 403 spoofed browser UAs.
UA = ("WebQAStream/0.1 (research/education project; "
      "+https://github.com/Skapis9999/Dynamic-Internet-based-Question-Answering-System) httpx")
# Footnote markers like [17] or [citation needed] would be confused with our own [n] citations.
FOOTNOTE_RE = re.compile(r"\[(?:\d+|[a-z]|citation needed|note \d+|edit)\]", re.IGNORECASE)

_http = httpx.Client(timeout=15, follow_redirects=True, headers={"User-Agent": UA})
_qdrant = QdrantClient(url=config.QDRANT_URL)

CHUNK_CHARS = 1000
OVERLAP_CHARS = 150
MAX_CHUNKS_PER_PAGE = 40
CACHE_TTL_S = 24 * 3600
# 429/503 mean "slow down" everywhere; Wikimedia also uses 403 for that ("robot policy").
# Other sites' 403s are permanent blocks, so retrying them only adds latency.
RETRY_STATUS = {429, 503}
RETRY_403_HOSTS = ("wikipedia.org", "wikimedia.org", "wikidata.org", "wiktionary.org")
RETRY_DELAYS = (1.0, 2.5)

# Politeness: at most 2 concurrent requests per host from one process/executor.
_host_locks: dict[str, threading.Semaphore] = defaultdict(lambda: threading.Semaphore(2))
_host_locks_guard = threading.Lock()


def _host_sem(url: str) -> threading.Semaphore:
    with _host_locks_guard:
        return _host_locks[urlparse(url).netloc]


def ensure_collection() -> None:
    if not _qdrant.collection_exists(config.QDRANT_COLLECTION):
        _qdrant.create_collection(
            config.QDRANT_COLLECTION,
            vectors_config=models.VectorParams(size=config.EMBED_DIM, distance=models.Distance.COSINE),
        )
        _qdrant.create_payload_index(config.QDRANT_COLLECTION, "url", models.PayloadSchemaType.KEYWORD)
        _qdrant.create_payload_index(config.QDRANT_COLLECTION, "fetched_at", models.PayloadSchemaType.FLOAT)


def cached_chunks(url: str) -> int:
    """Number of fresh chunks already stored for this URL (0 = not cached / stale)."""
    flt = models.Filter(must=[
        models.FieldCondition(key="url", match=models.MatchValue(value=url)),
        models.FieldCondition(key="fetched_at", range=models.Range(gte=time.time() - CACHE_TTL_S)),
    ])
    return _qdrant.count(config.QDRANT_COLLECTION, count_filter=flt, exact=True).count


def _get(url: str) -> httpx.Response:
    host = urlparse(url).netloc
    retry = RETRY_STATUS | ({403} if host.endswith(RETRY_403_HOSTS) else set())
    with _host_sem(url):
        for delay in (*RETRY_DELAYS, None):
            r = _http.get(url)
            if r.status_code not in retry or delay is None:
                return r
            time.sleep(delay)
    return r


def fetch_and_extract(url: str) -> tuple[str, str]:
    r = _get(url)
    r.raise_for_status()
    if "html" not in r.headers.get("content-type", "html"):
        raise ValueError(f"not html: {r.headers.get('content-type')}")
    text = trafilatura.extract(r.text, include_comments=False, include_tables=True, favor_recall=True) or ""
    text = FOOTNOTE_RE.sub("", text)
    meta = trafilatura.extract_metadata(r.text)
    if len(text) < 200:
        raise ValueError("no meaningful text extracted")
    return text, (meta.title if meta and meta.title else "")


def chunk(text: str) -> list[str]:
    """Paragraph-aware chunks of ~CHUNK_CHARS with a small overlap."""
    paras = [p.strip() for p in re.split(r"\n\s*\n|\n", text) if p.strip()]
    chunks, cur = [], ""
    for p in paras:
        if cur and len(cur) + len(p) + 1 > CHUNK_CHARS:
            chunks.append(cur)
            cur = cur[-OVERLAP_CHARS:] + " " + p
        else:
            cur = f"{cur}\n{p}" if cur else p
        while len(cur) > CHUNK_CHARS * 1.5:  # very long paragraph: hard split
            chunks.append(cur[:CHUNK_CHARS])
            cur = cur[CHUNK_CHARS - OVERLAP_CHARS:]
    if cur:
        chunks.append(cur)
    return chunks[:MAX_CHUNKS_PER_PAGE]


def embed_and_store(url: str, title: str, chunks: list[str]) -> int:
    if not chunks:
        return 0
    vectors = ollama.embed(chunks)
    # Drop an older version of this page first (it may have had more chunks).
    _qdrant.delete(config.QDRANT_COLLECTION, points_selector=models.FilterSelector(filter=models.Filter(
        must=[models.FieldCondition(key="url", match=models.MatchValue(value=url))])))
    now = time.time()
    _qdrant.upsert(config.QDRANT_COLLECTION, points=[
        models.PointStruct(
            id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{url}|{i}")),
            vector=v,
            payload={"url": url, "title": title, "idx": i, "text": c, "fetched_at": now},
        )
        for i, (c, v) in enumerate(zip(chunks, vectors))
    ])
    return len(chunks)


def process_url(url: str, title_hint: str = "") -> dict:
    """Full stage for one URL. Never raises: returns n_chunks, error, cached, seconds."""
    t0 = time.time()
    n, err, cached = 0, None, False
    try:
        n = cached_chunks(url)
        if n:
            cached = True
        else:
            text, title = fetch_and_extract(url)
            n = embed_and_store(url, title or title_hint, chunk(text))
    except Exception as e:
        err = f"{type(e).__name__}: {e}"[:300]
    return {"n_chunks": n, "error": err, "cached": cached, "seconds": round(time.time() - t0, 2)}
