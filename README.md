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
   │              Spark Structured Streaming (driver + 2 executor pods on k8s):
   │                         fetch → trafilatura → chunk → embed → Qdrant
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
| `spark-fetcher` | Spark Structured Streaming: page → clean text → chunks → vectors | `htmlToText` + `buildEncyclopaedia` (with `parallel = False`) |
| Qdrant | vector store | Elasticsearch (Haystack) |
| `answerer` | RAG with qwen3:8b, fast + final answers | extractive QA, stage2 / stage3 |
| Kafka | decouples every stage, gives parallelism | (sequential Python calls) |

## Running locally

Prerequisites: Docker Desktop with Kubernetes enabled (WSL 2.5+ so the VM uses cgroup v2),
Helm (`winget install Helm.Helm`), and Ollama on the host with these models:

```powershell
ollama pull qwen3:8b
ollama pull qwen3-embedding:0.6b
.\scripts\install-operators.ps1   # once: Strimzi, Spark Operator, KEDA
.\scripts\deploy.ps1              # build images + apply k8s/base
```

Then open http://localhost:8000. If the LoadBalancer isn't reachable, run
`kubectl -n wqa port-forward svc/api 8000:8000`.

Useful commands:

```powershell
kubectl -n wqa get pods
kubectl -n wqa logs -f deploy/answerer
kubectl -n wqa exec wqa-dual-role-0 -- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events
```

## Operators

| Operator | Manages | Resources in this repo |
|---|---|---|
| [Strimzi](https://strimzi.io) 1.3.0 | Kafka 4.2.2 (KRaft, single node) and its topics | `Kafka`, `KafkaNodePool`, `KafkaTopic` in `k8s/base/kafka.yaml` |
| [Spark Operator](https://github.com/kubeflow/spark-operator) 2.5.2 | Submitting and restarting the streaming job | `SparkApplication` in `k8s/base/spark.yaml` |
| [KEDA](https://keda.sh) 2.21.0 | Scaling `searcher` and `answerer` (1-3 replicas) on Kafka consumer lag | `ScaledObject` in `k8s/base/autoscaling.yaml` |

```powershell
kubectl -n wqa get kafka,kafkatopics,sparkapplication,scaledobject,hpa
```

## Spark on Kubernetes

The Spark Operator submits `wqa-fetcher` in cluster mode and resubmits it if it ever stops
(`restartPolicy: Always`). The driver asks the Kubernetes API for executor pods
(`wqa-fetcher-*-exec-N`) and removes them when it stops. Each micro-batch of URLs is spread over the executors, which share the same
`services/fetcher/pipeline.py` code as the plain-Python fetcher.

Executors produce each URL's result to Kafka as soon as it is done, instead of using Spark's
Kafka sink: the sink writes only when the whole micro-batch finishes, which made every page
(and the fast answer) wait for the slowest page.

Pages are cached in Qdrant by URL for 24 h, so a page fetched for one question is reused by the next.

```powershell
kubectl -n wqa port-forward svc/spark-ui 4040:4040     # Spark UI → http://localhost:4040
kubectl -n wqa logs -f deploy/spark-fetcher
```

To compare engines, swap them (never run both, or every URL is processed twice):

```powershell
kubectl -n wqa patch sparkapplication wqa-fetcher --type merge -p '{"spec":{"suspend":true}}'
kubectl -n wqa scale deploy/fetcher --replicas=3
```

## Evaluation

`python eval/run_eval.py` replays the 49 questions from the thesis and grades the final answers
with qwen3:8b against reference answers (`eval/questions.json`). First run (2026-11-03, Phase 2):

| | correct | correct + ½ partial |
|---|---|---|
| all 49 | 84% | 88% |
| factual (26) | 81% | 87% |
| open (17) | 94% | 97% |
| time-sensitive (5) | 60% | 60% |

Final answer: mean 21.0 s, median 20.3 s, p90 26.8 s after submitting the question.

Caveats: the judge is the same small model that writes the answers, and a manual check found it
marked at least two correct answers wrong (it trusts its outdated knowledge over the reference).
The thesis measured the share of all *candidate* answers that were correct (best ~46-48%); this
grades the single final answer, so the numbers are related but not directly comparable.

## Roadmap

- [x] **Phase 0**: end-to-end pipeline on k8s with plain Python workers
- [x] **Phase 1**: replace the fetcher with Spark Structured Streaming
- [x] **Phase 2**: URL cache, per-domain rate limiting, ambiguity handling, streaming tokens, evaluation on the thesis questions
- [x] **Phase 3**: Strimzi (Kafka operator), Spark Operator, KEDA autoscaling on consumer lag
- [ ] Unified search API (Tavily) instead of scraping engines through SearXNG
- [ ] **Phase 4**: Prometheus/Grafana, Kafka UI
