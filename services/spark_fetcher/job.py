"""The fetch -> extract -> chunk -> embed -> Qdrant stage as Spark Structured Streaming.

Kafka [urls] --(micro-batches)--> executors (pipeline.py per URL) --> Kafka [chunks-ready] + [events]

Results are produced to Kafka by the executors *as each URL finishes*, not through Spark's batch
Kafka sink: the sink only writes when the whole micro-batch is done, so every page (and the fast
answer) would wait for the slowest page in the batch. Spark still does the distribution,
offset tracking and checkpointing. Delivery is at-least-once: a task fails if its results weren't
delivered, so Spark retries it (fast, the pages are cached) and the answerer ignores duplicates.

Downstream services are unchanged: they can't tell whether pages were processed by the plain
Python fetcher or by Spark, except for the "engine"/"executor" fields on page events.
"""
import json
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from services.common import config

URL_SCHEMA = T.StructType([
    T.StructField("qid", T.StringType()),
    T.StructField("question", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("rank", T.IntegerType()),
    T.StructField("expected", T.IntegerType()),
    T.StructField("asked_at", T.DoubleType()),
])

PARTITIONS = int(os.getenv("SPARK_PARTITIONS", "4"))
THREADS_PER_PARTITION = int(os.getenv("SPARK_THREADS_PER_PARTITION", "4"))

MESSAGE_TIMEOUT_MS = 50_000
FLUSH_TIMEOUT_S = 60  # longer than the message timeout, so every message ends delivered or failed

_producer = None  # one Kafka producer per executor Python worker, reused across batches


def _get_producer():
    global _producer
    if _producer is None:
        from confluent_kafka import Producer
        _producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "linger.ms": 5,
                              "message.timeout.ms": MESSAGE_TIMEOUT_MS})
    return _producer


def process_partition(rows) -> None:
    """Runs on an executor. Network-bound work, so a small thread pool per partition."""
    from services.fetcher import pipeline  # imported on the executor, not the driver

    rows = [r.asDict() for r in rows]
    if not rows:
        return
    pipeline.ensure_collection()
    producer = _get_producer()
    host = socket.gethostname()
    failures = []

    def on_delivery(err, _msg):
        if err:
            failures.append(str(err))

    with ThreadPoolExecutor(THREADS_PER_PARTITION) as pool:
        futures = {pool.submit(pipeline.process_url, m["url"], m.get("title") or ""): m for m in rows}
        for fut in as_completed(futures):  # report each URL the moment it's done
            m, res = futures[fut], fut.result()
            ready = {"qid": m["qid"], "question": m["question"], "url": m["url"], "expected": m["expected"],
                     "asked_at": m.get("asked_at"), "n_chunks": res["n_chunks"], "error": res["error"]}
            event = {"qid": m["qid"], "type": "page", "ts": time.time(), "url": m["url"],
                     "engine": "spark", "executor": host, **res}
            producer.produce(config.T_EVENTS, key=m["qid"].encode(), value=json.dumps(event).encode(),
                             on_delivery=on_delivery)
            producer.produce(config.T_CHUNKS_READY, key=m["qid"].encode(), value=json.dumps(ready).encode(),
                             on_delivery=on_delivery)
            producer.poll(0)
    # Fail the task unless everything was delivered: otherwise Spark would commit the batch's
    # offsets and these results would be lost for good.
    undelivered = producer.flush(FLUSH_TIMEOUT_S)
    if undelivered or failures:
        raise RuntimeError(f"{undelivered} undelivered, {len(failures)} failed: {failures[:3]}")


def main() -> None:
    spark = SparkSession.builder.appName("wqa-fetcher").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    def handle_batch(batch_df, batch_id: int) -> None:
        t0 = time.time()
        (batch_df.select(F.from_json(F.col("value").cast("string"), URL_SCHEMA).alias("m")).select("m.*")
            .repartition(PARTITIONS)
            .foreachPartition(process_partition))
        print(f"batch {batch_id} done in {time.time() - t0:.1f}s", flush=True)

    (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.T_URLS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 64)
        .load()
        .writeStream
        .foreachBatch(handle_batch)
        .option("checkpointLocation", os.getenv("CHECKPOINT_DIR", "/checkpoints/fetcher"))
        .trigger(processingTime="500 milliseconds")
        .start()
        .awaitTermination())


if __name__ == "__main__":
    main()
