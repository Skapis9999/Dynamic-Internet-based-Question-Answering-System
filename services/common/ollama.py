"""Minimal Ollama client (chat + embeddings) over HTTP."""
import httpx

from . import config

_client = httpx.Client(base_url=config.OLLAMA_URL, timeout=httpx.Timeout(90, connect=10))


def chat(messages: list[dict], fmt: dict | str | None = None, temperature: float = 0.2) -> str:
    body = {
        "model": config.LLM_MODEL,
        "messages": messages,
        "stream": False,
        "think": False,  # qwen3: skip the reasoning trace, much faster
        "options": {"temperature": temperature, "num_ctx": config.LLM_NUM_CTX},
    }
    if fmt is not None:
        body["format"] = fmt
    r = _client.post("/api/chat", json=body)
    r.raise_for_status()
    return r.json()["message"]["content"]


def chat_stream(messages: list[dict], temperature: float = 0.2):
    """Yield content deltas as the model generates them."""
    import json
    body = {
        "model": config.LLM_MODEL, "messages": messages, "stream": True, "think": False,
        "options": {"temperature": temperature, "num_ctx": config.LLM_NUM_CTX},
    }
    with _client.stream("POST", "/api/chat", json=body) as r:
        r.raise_for_status()
        for line in r.iter_lines():
            if not line:
                continue
            chunk = json.loads(line)
            delta = chunk.get("message", {}).get("content", "")
            if delta:
                yield delta
            if chunk.get("done"):
                break


def embed(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    r = _client.post("/api/embed", json={"model": config.EMBED_MODEL, "input": texts})
    r.raise_for_status()
    return r.json()["embeddings"]
