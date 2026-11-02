"""Pure page-processing functions, shared by the plain fetcher and (later) the Spark job."""
import re
import uuid

import httpx
import trafilatura
from qdrant_client import QdrantClient, models

from services.common import config, ollama

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
_http = httpx.Client(timeout=15, follow_redirects=True, headers={"User-Agent": UA})
_qdrant = QdrantClient(url=config.QDRANT_URL)

CHUNK_CHARS = 1000
OVERLAP_CHARS = 150
MAX_CHUNKS_PER_PAGE = 40


def ensure_collection() -> None:
    if not _qdrant.collection_exists(config.QDRANT_COLLECTION):
        _qdrant.create_collection(
            config.QDRANT_COLLECTION,
            vectors_config=models.VectorParams(size=config.EMBED_DIM, distance=models.Distance.COSINE),
        )
        _qdrant.create_payload_index(config.QDRANT_COLLECTION, "qid", models.PayloadSchemaType.KEYWORD)


def fetch_and_extract(url: str) -> tuple[str, str]:
    r = _http.get(url)
    r.raise_for_status()
    if "html" not in r.headers.get("content-type", "html"):
        raise ValueError(f"not html: {r.headers.get('content-type')}")
    text = trafilatura.extract(r.text, include_comments=False, include_tables=True, favor_recall=True) or ""
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


def embed_and_store(qid: str, url: str, title: str, rank: int, chunks: list[str]) -> int:
    if not chunks:
        return 0
    vectors = ollama.embed(chunks)
    _qdrant.upsert(config.QDRANT_COLLECTION, points=[
        models.PointStruct(
            id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{qid}|{url}|{i}")),
            vector=v,
            payload={"qid": qid, "url": url, "title": title, "rank": rank, "idx": i, "text": c},
        )
        for i, (c, v) in enumerate(zip(chunks, vectors))
    ])
    return len(chunks)
