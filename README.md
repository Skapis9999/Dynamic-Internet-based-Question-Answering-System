# Dynamic Internet-based Question-Answering System

Ask a question; the system searches the live web, reads the pages in parallel, and answers with
citations using a local LLM. A streaming, Kubernetes-native rebuild of my diploma thesis
([Skapis9999/DiplomaThesis](https://github.com/Skapis9999/DiplomaThesis)), using Kafka, Spark and Ollama.

## Architecture

```
Browser ──POST /api/ask──► api ──► [questions]
   ▲                                   │
   │ SSE                               ▼
   │                         searcher: qwen3 writes search queries → SearXNG → top URLs
   │                                   │  one message per URL, keyed by URL
   │                                   ▼
   │                                [urls]  (3 partitions)
   │                                   │
   │                         fetcher ×3: fetch → trafilatura → chunk → embed → Qdrant
   │                                   │
   │                                   ▼
   │                            [chunks-ready]
   │                                   │
   │                         answerer: retrieve top-k → qwen3 answer w/ [n] citations
   │                                   │   "fast" after first page, "final" when all done
   └────────── [events] ◄──────────────┘   (every stage reports progress here)
```

| Component | Role | Thesis equivalent |
|---|---|---|
| `searcher` | LLM query planning + SearXNG metasearch | `findImportantWord` + `googlesearch` |
| `fetcher` | page → clean text → chunks → vectors | `htmlToText` + `buildEncyclopaedia` |
| Qdrant | vector store | Elasticsearch (Haystack) |
| `answerer` | RAG with qwen3:8b, fast + final answers | extractive QA, stage2 / stage3 |
| Kafka | decouples every stage, gives parallelism | (sequential Python calls) |

## Running locally

Prerequisites: Docker Desktop with Kubernetes enabled, and Ollama on the host with these models:

```powershell
ollama pull qwen3:8b
ollama pull qwen3-embedding:0.6b
.\scripts\deploy.ps1
```

Then open http://localhost:8000. If the LoadBalancer isn't reachable, run
`kubectl -n wqa port-forward svc/api 8000:8000`.

Useful commands:

```powershell
kubectl -n wqa get pods
kubectl -n wqa logs -f deploy/answerer
kubectl -n wqa scale deploy/fetcher --replicas=3
kubectl -n wqa exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events
```

## Roadmap

- [x] **Phase 0**: end-to-end pipeline on k8s with plain Python workers
- [ ] **Phase 1**: replace the fetcher with Spark Structured Streaming
- [ ] **Phase 2**: URL cache, reranking, streaming tokens, evaluation on the thesis questions
- [ ] **Phase 3**: Strimzi (Kafka operator), Spark Operator, KEDA autoscaling on consumer lag
- [ ] **Phase 4**: Prometheus/Grafana, Kafka UI
