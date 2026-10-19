import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

from app.main import app


class RecordingPublisher:
    def __init__(self):
        self.events = []

    def publish(self, event):
        self.events.append(event)


@pytest.fixture
def publisher():
    return RecordingPublisher()


@pytest.fixture
def database_url(monkeypatch):
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.fail("Set TEST_DATABASE_URL to a PostgreSQL database (see README).")
    schema = f"careflow_test_{uuid4().hex}"
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        test_url = make_conninfo(url, options=f"-csearch_path={schema}")
        try:
            with psycopg.connect(test_url) as connection:
                for filename in ("schema.sql", "notification_schema.sql", "messaging_schema.sql"):
                    connection.execute(
                        (Path(__file__).parents[1] / "app" / filename).read_text()
                    )
            monkeypatch.setenv("DATABASE_URL", test_url)
            yield test_url
        finally:
            # Only this test's randomly named schema is removed.
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


@pytest.fixture
def connection(database_url):
    with psycopg.connect(database_url, autocommit=True, row_factory=dict_row) as conn:
        yield conn


@pytest.fixture
def client(database_url):
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client


@pytest.fixture
def event():
    return {
        "event_id": "evt_001",
        "patient_id": "patient_123",
        "appointment_id": "appt_456",
        "event_type": "APPOINTMENT_SCHEDULED",
        "event_version": 1,
        "occurred_at": "2026-10-17T10:00:00Z",
    }


@pytest.fixture
def kafka():
    from time import monotonic
    from types import SimpleNamespace

    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic

    from app.kafka import TOPIC
    from app.kafka_producer import KafkaPublisher
    from app.worker import create_consumer

    servers = os.environ.get("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not servers:
        pytest.skip("Set TEST_KAFKA_BOOTSTRAP_SERVERS to run real Kafka tests")
    admin = AdminClient({"bootstrap.servers": servers})
    topic = f"careflow-test-{uuid4().hex}"
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(15)
    consumers = []

    def consumer(group=None):
        result = create_consumer(servers, group_id=group or f"test-{uuid4().hex}", topic=topic)
        consumers.append(result)
        return result

    def reader():
        result = Consumer({
            "bootstrap.servers": servers,
            "group.id": f"reader-{uuid4().hex}",
            "enable.auto.commit": False,
        })
        result.assign([TopicPartition(topic, partition, 0) for partition in range(3)])
        consumers.append(result)
        return result

    def close(result):
        result.close()
        consumers.remove(result)

    def read(result):
        deadline = monotonic() + 15
        while monotonic() < deadline:
            record = result.poll(0.2)
            if record is not None:
                assert record.error() is None
                return record
        pytest.fail("Kafka consumer did not receive a record")

    def offset(result, record):
        position = result.committed([TopicPartition(topic, record.partition())], timeout=5)[0]
        assert position.error is None
        return position.offset

    try:
        metadata = admin.list_topics(TOPIC, timeout=10).topics[TOPIC]
        assert metadata.error is None
        assert len(metadata.partitions) == 3
        yield SimpleNamespace(
            publisher=KafkaPublisher(servers, topic=topic), consumer=consumer,
            reader=reader, close=close, read=read, offset=offset, topic=topic,
        )
    finally:
        for result in consumers:
            result.close()
        admin.delete_topics([topic])[topic].result(15)
