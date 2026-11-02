"""Phase 1: the fetch -> extract -> chunk -> embed -> Qdrant stage as Spark Structured Streaming.

Kafka [urls] --(micro-batches)--> executors (pipeline.py per URL) --> Kafka [chunks-ready] + [events]

Downstream services are unchanged: they can't tell whether pages were processed by the plain
Python fetcher or by Spark, except for the "engine"/"executor" fields on page events.
"""
import json
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor

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
])
OUT_SCHEMA = T.StructType([
    T.StructField("topic", T.StringType()),
    T.StructField("key", T.StringType()),
    T.StructField("value", T.StringType()),
])

PARTITIONS = int(os.getenv("SPARK_PARTITIONS", "4"))
THREADS_PER_PARTITION = int(os.getenv("SPARK_THREADS_PER_PARTITION", "4"))


def process_partition(rows):
    """Runs on an executor. Network-bound work, so a small thread pool per partition."""
    from services.fetcher import pipeline  # imported on the executor, not the driver

    rows = [r.asDict() for r in rows]
    if not rows:
        return
    pipeline.ensure_collection()
    host = socket.gethostname()

    def work(m):
        t0 = time.time()
        n, err = 0, None
        try:
            text, title = pipeline.fetch_and_extract(m["url"])
            n = pipeline.embed_and_store(m["qid"], m["url"], title or m.get("title") or "",
                                         m.get("rank") or 0, pipeline.chunk(text))
        except Exception as e:
            err = f"{type(e).__name__}: {e}"[:300]
        return m, n, err, round(time.time() - t0, 2)

    with ThreadPoolExecutor(THREADS_PER_PARTITION) as pool:
        for m, n, err, secs in pool.map(work, rows):
            ready = {"qid": m["qid"], "question": m["question"], "url": m["url"],
                     "n_chunks": n, "expected": m["expected"], "error": err}
            event = {"qid": m["qid"], "type": "page", "ts": time.time(), "url": m["url"],
                     "n_chunks": n, "error": err, "engine": "spark", "executor": host, "seconds": secs}
            yield (config.T_CHUNKS_READY, m["qid"], json.dumps(ready))
            yield (config.T_EVENTS, m["qid"], json.dumps(event))


def main() -> None:
    spark = SparkSession.builder.appName("wqa-fetcher").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    def handle_batch(batch_df, batch_id: int) -> None:
        msgs = batch_df.select(F.from_json(F.col("value").cast("string"), URL_SCHEMA).alias("m")).select("m.*")
        # Single pass over the data: each URL yields both of its output records.
        out = msgs.repartition(PARTITIONS).rdd.mapPartitions(process_partition)
        t0 = time.time()
        (spark.createDataFrame(out, OUT_SCHEMA)
            .write.format("kafka")
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
            .save())
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
