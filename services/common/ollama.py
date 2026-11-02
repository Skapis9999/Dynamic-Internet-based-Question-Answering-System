"""Minimal Ollama client (chat + embeddings) over HTTP."""
import httpx

from . import config

_client = httpx.Client(base_url=config.OLLAMA_URL, timeout=httpx.Timeout(300, connect=10))


def chat(messages: list[dict], fmt: dict | str | None = None, temperature: float = 0.2) -> str:
    body = {
        "model": config.LLM_MODEL,
        "messages": messages,
        "stream": False,
        "think": False,  # qwen3: skip the reasoning trace, much faster
        "options": {"temperature": temperature, "num_ctx": 8192},
    }
    if fmt is not None:
        body["format"] = fmt
    r = _client.post("/api/chat", json=body)
    r.raise_for_status()
    return r.json()["message"]["content"]


def embed(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    r = _client.post("/api/embed", json={"model": config.EMBED_MODEL, "input": texts})
    r.raise_for_status()
    return r.json()["embeddings"]
