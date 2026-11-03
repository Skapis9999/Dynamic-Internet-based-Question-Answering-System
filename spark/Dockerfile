# Spark image used for BOTH the driver pod and the executor pods it launches.
FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

USER root

# Kafka connector for Structured Streaming (+ its runtime deps), baked in so nothing
# is downloaded at job start.
ARG SPARK_VERSION=4.1.3
ARG MVN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars \
 && curl -fsSLO $MVN/org/apache/spark/spark-sql-kafka-0-10_2.13/$SPARK_VERSION/spark-sql-kafka-0-10_2.13-$SPARK_VERSION.jar \
 && curl -fsSLO $MVN/org/apache/spark/spark-token-provider-kafka-0-10_2.13/$SPARK_VERSION/spark-token-provider-kafka-0-10_2.13-$SPARK_VERSION.jar \
 && curl -fsSLO $MVN/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
 && curl -fsSLO $MVN/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar

COPY spark/requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt \
 || python3 -m pip install --no-cache-dir --break-system-packages -r /tmp/requirements.txt

WORKDIR /app
COPY services ./services
RUN mkdir -p /checkpoints && chown -R spark:spark /checkpoints

ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
USER spark
# ENTRYPOINT stays the image's /opt/entrypoint.sh: executor pods rely on it.
