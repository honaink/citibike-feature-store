"""Kafka client configuration for the local broker (plaintext) and MSK Serverless (IAM)."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from citibike_fs.config import Settings, get_settings

log = logging.getLogger(__name__)

TOPIC_RETENTION_MS = str(24 * 60 * 60 * 1000)


def _client_config(settings: Settings) -> dict[str, Any]:
    conf: dict[str, Any] = {"bootstrap.servers": settings.kafka_bootstrap_servers}
    if settings.kafka_auth == "msk_iam":
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        def oauth_cb(_config: str) -> tuple[str, float]:
            token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(settings.aws_region)
            return token, expiry_ms / 1000

        conf.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "OAUTHBEARER",
                "oauth_cb": oauth_cb,
            }
        )
    return conf


def spark_kafka_options(settings: Settings | None = None) -> dict[str, str]:
    """Options for Spark's Kafka source, mirroring `_client_config`."""
    settings = settings or get_settings()
    opts = {"kafka.bootstrap.servers": settings.kafka_bootstrap_servers}
    if settings.kafka_auth == "msk_iam":
        opts.update(
            {
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": "AWS_MSK_IAM",
                "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
                "kafka.sasl.client.callback.handler.class": (
                    "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
                ),
            }
        )
    return opts


def ensure_topics(topics: list[str], settings: Settings | None = None, retries: int = 30) -> None:
    """Create topics if missing. MSK Serverless does not auto-create them."""
    settings = settings or get_settings()
    admin = AdminClient(_client_config(settings))
    for attempt in range(1, retries + 1):
        try:
            admin.poll(1)  # lets the OAuth callback run before the first request
            existing = set(admin.list_topics(timeout=10).topics)
            break
        except KafkaException as exc:
            if attempt == retries:
                raise
            log.warning("Kafka not reachable yet (%s); retry %d/%d", exc, attempt, retries)
            time.sleep(5)
    missing = [t for t in topics if t not in existing]
    if not missing:
        return
    new_topics = [
        NewTopic(
            t, num_partitions=1, replication_factor=-1, config={"retention.ms": TOPIC_RETENTION_MS}
        )
        for t in missing
    ]
    for topic, future in admin.create_topics(new_topics).items():
        try:
            future.result()
            log.info("created topic %s", topic)
        except KafkaException as exc:
            # Another producer may have created it between the list and the create.
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


class JsonProducer:
    """Thin wrapper: JSON values, string keys, errors surfaced in the log."""

    def __init__(self, settings: Settings | None = None):
        self._producer = Producer(
            {
                **_client_config(settings or get_settings()),
                "linger.ms": 50,
                "enable.idempotence": True,
            }
        )

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err is not None:
            log.error("delivery failed for %s: %s", msg.topic(), err)

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        while True:
            try:
                self._producer.produce(
                    topic, key=key, value=json.dumps(value).encode(), on_delivery=self._on_delivery
                )
                break
            except BufferError:
                self._producer.poll(1)
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(30)
