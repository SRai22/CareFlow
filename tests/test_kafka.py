import json

import psycopg
import pytest
from confluent_kafka import TopicPartition

from app.outbox_publisher import publish_one
from app.schemas import EventCreate


def test_api_outbox_kafka_key_and_http_replay(client, event, connection, kafka):
    event = {**event, "appointment_id": "appt_123"}
    response = client.post("/events", json=event)
    assert response.status_code == 201
    assert client.get("/appointments/appt_123").json()["status"] == "SCHEDULED"
    assert connection.execute("SELECT published_at FROM outbox").fetchone()["published_at"] is None
    assert publish_one(connection, kafka.publisher)
    reader = kafka.reader()
    record = kafka.read(reader)
    assert record.key() == b"appt_123"
    assert json.loads(record.value()) == EventCreate(**event).model_dump(mode="json")
    retry = client.post("/events", json=event)
    assert retry.status_code == 201
    assert retry.content == response.content
    assert not publish_one(connection, kafka.publisher)
    assert sum(reader.get_watermark_offsets(TopicPartition(kafka.topic, p), timeout=5)[1] for p in range(3)) == 1


def test_broker_ack_then_database_failure_causes_real_kafka_replay(client, event, connection, kafka):
    assert client.post("/events", json=event).status_code == 201
    # Publication succeeds, then marking published fails: PostgreSQL rolls back, Kafka cannot.
    connection.execute("ALTER TABLE outbox ADD CONSTRAINT injected_crash CHECK (published_at IS NULL)")
    with pytest.raises(psycopg.errors.CheckViolation):
        publish_one(connection, kafka.publisher)
    assert connection.execute("SELECT published_at FROM outbox").fetchone()["published_at"] is None
    connection.execute("ALTER TABLE outbox DROP CONSTRAINT injected_crash")
    assert publish_one(connection, kafka.publisher)
    reader = kafka.reader()
    first, duplicate = kafka.read(reader), kafka.read(reader)
    assert first.key() == duplicate.key() == event["appointment_id"].encode()
    assert first.value() == duplicate.value()
    assert first.partition() == duplicate.partition()
    assert duplicate.offset() == first.offset() + 1
