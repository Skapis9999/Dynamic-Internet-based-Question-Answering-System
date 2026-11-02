"""Shared configuration, read from environment (populated by the k8s ConfigMap)."""
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434")
QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
SEARXNG_URL = os.getenv("SEARXNG_URL", "http://searxng:8080")

LLM_MODEL = os.getenv("LLM_MODEL", "qwen3:8b")
EMBED_MODEL = os.getenv("EMBED_MODEL", "qwen3-embedding:0.6b")
EMBED_DIM = int(os.getenv("EMBED_DIM", "1024"))
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "chunks")

# How many search queries the planner generates, and URLs kept per question.
NUM_QUERIES = int(os.getenv("NUM_QUERIES", "3"))
MAX_URLS = int(os.getenv("MAX_URLS", "8"))
# Final answer is produced when all pages are processed or this many seconds pass.
ANSWER_DEADLINE_S = float(os.getenv("ANSWER_DEADLINE_S", "40"))
TOP_K = int(os.getenv("TOP_K", "6"))

# Kafka topics
T_QUESTIONS = "questions"        # key=qid  {qid, question}
T_URLS = "urls"                  # key=url  {qid, question, url, title, rank, expected}
T_CHUNKS_READY = "chunks-ready"  # key=qid  {qid, question, url, n_chunks, expected, error?}
T_EVENTS = "events"              # key=qid  {qid, type, ...}  -> streamed to the UI
