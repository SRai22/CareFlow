import json
import os
from time import monotonic
from types import SimpleNamespace
from uuid import uuid4

import pytest
from confluent_kafka import OFFSET_INVALID, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from app import notifications, worker
from app.kafka import KafkaPublisher
from app.migrate_phase2 import migrate
from app.mock_provider import ProviderError
from app.schemas import EventCreate


@pytest.fixture
def kafka_notifications(client, publisher, monkeypatch):
    servers = os.environ.get("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not servers:
        pytest.skip("Set TEST_KAFKA_BOOTSTRAP_SERVERS to run real Kafka tests")
    topic = f"careflow-test-{uuid4().hex}"
    group = f"notifications-test-{uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": servers})
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(15)
    actual = KafkaPublisher(servers, topic=topic)
    monkeypatch.setattr(publisher, "publish", actual.publish)
    consumers = []

    def create():
        consumer = worker.create_consumer(servers, group_id=group, topic=topic)
        consumers.append(consumer)
        return consumer

    def restart(consumer):
        consumer.close()
        consumers.remove(consumer)
        return create()

    try:
        yield SimpleNamespace(publisher=actual, consumer=create, restart=restart)
    finally:
        for consumer in consumers:
            consumer.close()
        admin.delete_topics([topic])[topic].result(15)


def read_record(consumer):
    deadline = monotonic() + 15
    while monotonic() < deadline:
        record = consumer.poll(0.2)
        if record is not None:
            assert record.error() is None
            return record
    pytest.fail("Notification consumer did not receive a record")


def committed_offset(consumer, record):
    position = consumer.committed(
        [TopicPartition(record.topic(), record.partition())], timeout=5
    )[0]
    assert position.error is None
    return position.offset


def test_duplicate_kafka_records_send_one_notification(
    client, event, connection, kafka_notifications
):
    assert client.post("/events", json=event).status_code == 201
    kafka_notifications.publisher.publish(EventCreate(**event))
    consumer = kafka_notifications.consumer()
    first = read_record(consumer)
    worker.process_record(consumer, first)
    duplicate = read_record(consumer)
    assert duplicate.partition() == first.partition()
    assert duplicate.offset() == first.offset() + 1
    worker.process_record(consumer, duplicate)

    assert connection.execute("SELECT count(*) FROM notifications").fetchone()["count"] == 1
    assert connection.execute("SELECT status FROM notifications").fetchone()["status"] == "SENT"
    assert connection.execute("SELECT count(*) FROM mock_provider_acceptances").fetchone()["count"] == 1
    assert committed_offset(consumer, duplicate) == duplicate.offset() + 1


def test_consumer_restart_recovers_committed_offset(
    client, event, connection, kafka_notifications
):
    assert client.post("/events", json=event).status_code == 201
    consumer = kafka_notifications.consumer()
    first = read_record(consumer)
    worker.process_record(consumer, first)
    consumer = kafka_notifications.restart(consumer)
    assert committed_offset(consumer, first) == first.offset() + 1

    second = {**event, "event_id": "evt_second", "appointment_id": "appt_other"}
    assert client.post("/events", json=second).status_code == 201
    record = read_record(consumer)
    assert json.loads(record.value())["event_id"] == second["event_id"]
    worker.process_record(consumer, record)
    assert connection.execute("SELECT count(*) FROM notifications").fetchone()["count"] == 2


def test_independent_appointments_have_independent_notifications(
    client, event, connection, kafka_notifications
):
    other = {**event, "event_id": "evt_other", "appointment_id": "appt_other"}
    for payload in (event, other):
        assert client.post("/events", json=payload).status_code == 201
    consumer = kafka_notifications.consumer()
    keys = set()
    for _ in range(2):
        record = read_record(consumer)
        keys.add(record.key())
        worker.process_record(consumer, record)
    assert keys == {b"appt_456", b"appt_other"}
    outcomes = connection.execute("SELECT * FROM notifications").fetchall()
    assert {row["event_id"] for row in outcomes} == {event["event_id"], other["event_id"]}
    assert all(row["status"] == "SENT" for row in outcomes)
    assert len({row["provider_key"] for row in outcomes}) == 2
    assert connection.execute("SELECT count(*) FROM mock_provider_acceptances").fetchone()["count"] == 2


def test_repeated_cancellation_is_published_but_does_not_warrant_another_notice(
    client, event, connection, kafka_notifications
):
    assert client.post("/events", json=event).status_code == 201
    for version in (2, 3):
        payload = {
            **event,
            "event_id": f"evt_cancel_{version}",
            "event_type": "APPOINTMENT_CANCELLED",
            "event_version": version,
        }
        assert client.post("/events", json=payload).status_code == 201

    consumer = kafka_notifications.consumer()
    for _ in range(3):
        record = read_record(consumer)
        worker.process_record(consumer, record)
    assert committed_offset(consumer, record) == record.offset() + 1
    outcomes = connection.execute("SELECT * FROM notifications").fetchall()
    assert {row["event_id"] for row in outcomes} == {"evt_001", "evt_cancel_2"}
    assert connection.execute("SELECT count(*) FROM mock_provider_acceptances").fetchone()["count"] == 2
    assert connection.execute(
        "SELECT notification_type FROM events WHERE event_id = 'evt_cancel_3'"
    ).fetchone()["notification_type"] is None


def test_crash_after_provider_acceptance_recovers_without_duplicate_send(
    client, event, connection, kafka_notifications, monkeypatch
):
    assert client.post("/events", json=event).status_code == 201
    consumer = kafka_notifications.consumer()
    record = read_record(consumer)

    def crash_before_marking_sent(*args):
        raise RuntimeError("Injected crash after provider acceptance")

    with monkeypatch.context() as patch:
        patch.setattr(notifications, "mark_sent", crash_before_marking_sent)
        with pytest.raises(RuntimeError, match="after provider acceptance"):
            worker.process_record(consumer, record)

    pending = connection.execute("SELECT * FROM notifications").fetchone()
    accepted = connection.execute("SELECT * FROM mock_provider_acceptances").fetchone()
    assert pending["status"] == "PENDING"
    assert accepted["idempotency_key"] == pending["provider_key"]
    assert committed_offset(consumer, record) == OFFSET_INVALID

    # New Kafka consumer and new database connections; provider acceptance survives.
    consumer = kafka_notifications.restart(consumer)
    retry = read_record(consumer)
    assert (retry.partition(), retry.offset()) == (record.partition(), record.offset())
    worker.process_record(consumer, retry)
    sent = connection.execute("SELECT * FROM notifications").fetchone()
    assert sent["status"] == "SENT"
    assert sent["provider_message_id"] == accepted["message_id"]
    assert sent["created_at"] == pending["created_at"]
    assert connection.execute("SELECT count(*) FROM mock_provider_acceptances").fetchone()["count"] == 1
    assert committed_offset(consumer, retry) == retry.offset() + 1


def test_provider_failure_retries_three_times_without_advancing_offset(
    client, event, connection, kafka_notifications, monkeypatch
):
    assert client.post("/events", json=event).status_code == 201
    consumer = kafka_notifications.consumer()
    record = read_record(consumer)
    attempts = []

    def unavailable(key, payload):
        attempts.append(key)
        raise ProviderError("Injected provider outage")

    with monkeypatch.context() as patch:
        patch.setattr(notifications, "send_notification", unavailable)
        patch.setattr(worker, "sleep", lambda seconds: None)
        with pytest.raises(ProviderError):
            worker.process_record(consumer, record)

    assert len(attempts) == 3
    assert len(set(attempts)) == 1
    assert committed_offset(consumer, record) == OFFSET_INVALID
    assert connection.execute("SELECT status FROM notifications").fetchone()["status"] == "PENDING"
    consumer = kafka_notifications.restart(consumer)
    retry = read_record(consumer)
    worker.process_record(consumer, retry)
    assert connection.execute("SELECT status FROM notifications").fetchone()["status"] == "SENT"
    assert committed_offset(consumer, retry) == retry.offset() + 1


def test_invalid_record_does_not_commit_and_reappears_after_restart(kafka_notifications):
    producer = kafka_notifications.publisher.producer
    producer.produce(kafka_notifications.publisher.topic, key=b"appt_bad", value=b"{}")
    assert producer.flush(6) == 0
    consumer = kafka_notifications.consumer()
    record = read_record(consumer)
    with pytest.raises(ValueError):
        worker.process_record(consumer, record)
    assert committed_offset(consumer, record) == OFFSET_INVALID
    consumer = kafka_notifications.restart(consumer)
    retry = read_record(consumer)
    assert (retry.partition(), retry.offset()) == (record.partition(), record.offset())


def test_phase2_migration_preserves_events_without_sending_historical_notices(
    client, event, connection
):
    original = client.post("/events", json=event)
    assert original.status_code == 201
    connection.execute("DROP TABLE notifications, mock_provider_acceptances")
    connection.execute("ALTER TABLE events DROP COLUMN notification_type")
    migrate(connection)
    assert client.post("/events", json=event).content == original.content
    assert connection.execute("SELECT notification_type FROM events").fetchone()["notification_type"] is None
    assert connection.execute("SELECT count(*) FROM notifications").fetchone()["count"] == 0
