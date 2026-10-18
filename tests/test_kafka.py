import json
import os
from time import monotonic
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from app.kafka import TOPIC, KafkaPublisher, PublishError
from app.schemas import EventCreate


@pytest.fixture
def kafka_reader():
    servers = os.environ.get("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not servers:
        pytest.skip("Set TEST_KAFKA_BOOTSTRAP_SERVERS to run real Kafka tests")
    admin = AdminClient({"bootstrap.servers": servers})
    test_topic = f"careflow-acceptance-{uuid4().hex}"
    admin.create_topics([NewTopic(test_topic, num_partitions=3, replication_factor=1)])[
        test_topic
    ].result(15)
    consumer = Consumer(
        {
            "bootstrap.servers": servers,
            "group.id": f"acceptance-{uuid4().hex}",
            "enable.auto.commit": False,
            "allow.auto.create.topics": False,
        }
    )
    try:
        topic = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC]
        assert topic.error is None
        assert len(topic.partitions) == 3
        consumer.assign([TopicPartition(test_topic, partition, 0) for partition in range(3)])
        yield consumer
    finally:
        consumer.close()
        admin.delete_topics([test_topic])[test_topic].result(15)


def read_event(consumer, event_id, timeout=10):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        record = consumer.poll(0.2)
        if record is None:
            continue
        assert record.error() is None
        if json.loads(record.value())["event_id"] == event_id:
            return record
    pytest.fail(f"No Kafka record for {event_id}")


def test_post_commits_then_publishes_keyed_event_once(
    client, event, connection, monkeypatch, publisher, kafka_reader
):
    test_topic = kafka_reader.assignment()[0].topic
    real_publisher = KafkaPublisher(os.environ["TEST_KAFKA_BOOTSTRAP_SERVERS"], topic=test_topic)
    monkeypatch.setattr(publisher, "publish", real_publisher.publish)
    event = {**event, "event_id": f"evt_{uuid4().hex}", "appointment_id": "appt_123"}

    response = client.post("/events", json=event)
    assert response.status_code == 201
    assert client.get("/appointments/appt_123").json()["status"] == "SCHEDULED"
    record = read_event(kafka_reader, event["event_id"])
    assert record.topic() == test_topic
    assert record.key() == b"appt_123"
    assert json.loads(record.value()) == EventCreate(**event).model_dump(mode="json")

    retry = client.post("/events", json=event)
    assert retry.status_code == 201
    assert retry.content == response.content
    assert sum(
        kafka_reader.get_watermark_offsets(TopicPartition(test_topic, partition), timeout=5)[1]
        for partition in range(3)
    ) == 1
    assert connection.execute("SELECT count(*) FROM events").fetchone()["count"] == 1


def test_publication_failure_leaves_committed_event_and_replay_does_not_republish(
    client, event, connection, publisher, monkeypatch
):
    attempts = []

    def fail_publish(payload):
        attempts.append(payload.event_id)
        # A separate request/connection can already read the committed appointment.
        assert client.get(f"/appointments/{payload.appointment_id}").status_code == 200
        raise PublishError("Injected Kafka failure")

    monkeypatch.setattr(publisher, "publish", fail_publish)
    response = client.post("/events", json=event)
    assert response.status_code == 503
    assert "Event committed" in response.json()["detail"]
    assert connection.execute("SELECT count(*) FROM events").fetchone()["count"] == 1
    assert client.post("/events", json=event).status_code == 201
    assert attempts == [event["event_id"]]
