"""Thin Kafka helpers on top of confluent-kafka (JSON values, string keys)."""
import json
import logging
import signal
import time
from typing import Callable, Iterable

from confluent_kafka import Consumer, KafkaError, Producer

from . import config

log = logging.getLogger(__name__)


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "linger.ms": 5})


def send(producer: Producer, topic: str, key: str, value: dict) -> None:
    producer.produce(topic, key=key.encode(), value=json.dumps(value).encode())
    producer.poll(0)


def emit_event(producer: Producer, qid: str, type_: str, **fields) -> None:
    """Progress/answer events that the API streams to the browser."""
    send(producer, config.T_EVENTS, qid, {"qid": qid, "type": type_, "ts": time.time(), **fields})


def make_consumer(group: str, topics: Iterable[str], offset_reset: str = "earliest") -> Consumer:
    c = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": offset_reset,
        "enable.auto.commit": True,
        # LLM calls can take a while; don't get kicked out of the group.
        "max.poll.interval.ms": 600_000,
    })
    c.subscribe(list(topics))
    return c


def run_loop(consumer: Consumer, handler: Callable[[str, dict], None],
             idle: Callable[[], None] | None = None, poll_s: float = 1.0) -> None:
    """Poll forever, call handler(topic, value) per message, idle() between polls."""
    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    try:
        while not stop["flag"]:
            msg = consumer.poll(poll_s)
            if msg is not None and msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("kafka error: %s", msg.error())
            elif msg is not None:
                try:
                    handler(msg.topic(), json.loads(msg.value()))
                except Exception:
                    log.exception("handler failed for message on %s", msg.topic())
            if idle:
                try:
                    idle()
                except Exception:
                    log.exception("idle hook failed")
    finally:
        consumer.close()


def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger(name)
