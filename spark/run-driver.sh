#!/bin/bash
# Runs inside the driver pod: spark-submit in *client* mode against the in-cluster
# Kubernetes API, so Spark itself creates (and cleans up) the executor pods.
set -euo pipefail

SA=/var/run/secrets/kubernetes.io/serviceaccount

# Forward our service config to the executors.
EXEC_ENV=()
for v in KAFKA_BOOTSTRAP QDRANT_URL OLLAMA_URL EMBED_MODEL EMBED_DIM QDRANT_COLLECTION; do
  if [ -n "${!v:-}" ]; then EXEC_ENV+=(--conf "spark.executorEnv.$v=${!v}"); fi
done

exec /opt/spark/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc:443 \
  --deploy-mode client \
  --name wqa-fetcher \
  --driver-memory 512m \
  --conf spark.kubernetes.namespace="$POD_NAMESPACE" \
  --conf spark.kubernetes.driver.pod.name="$POD_NAME" \
  --conf spark.kubernetes.authenticate.caCertFile=$SA/ca.crt \
  --conf spark.kubernetes.authenticate.oauthTokenFile=$SA/token \
  --conf spark.kubernetes.container.image="$SPARK_IMAGE" \
  --conf spark.kubernetes.container.image.pullPolicy=Always \
  --conf spark.kubernetes.executor.label.part-of=wqa-spark \
  --conf spark.kubernetes.executor.request.cores=250m \
  --conf spark.driver.host="$POD_IP" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=7078 \
  --conf spark.blockManager.port=7079 \
  --conf spark.executor.instances="${SPARK_EXECUTORS:-2}" \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=512m \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.ui.showConsoleProgress=false \
  "${EXEC_ENV[@]}" \
  /app/services/spark_fetcher/job.py
