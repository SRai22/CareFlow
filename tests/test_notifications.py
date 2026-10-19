import json
from uuid import uuid4

import psycopg
import pytest
from confluent_kafka import KafkaException, OFFSET_INVALID

from app import worker
from app.consumers import analytics, notifications
from app.migrate_phase2 import migrate
from app.outbox_publisher import publish_one


@pytest.mark.parametrize("handler,table", [
    (notifications.process_event, "notifications"),
    (analytics.process_event, "analytics_events"),
])
def test_duplicate_deliveries_persist_once(client, event, connection, kafka, handler, table):
    assert client.post("/events", json=event).status_code == 201
    assert publish_one(connection, kafka.publisher)
    kafka.publisher.publish(connection.execute("SELECT * FROM outbox").fetchone())
    consumer = kafka.consumer()
    for _ in range(2):
        record = kafka.read(consumer)
        worker.process_record(consumer, record, handler)
    assert kafka.offset(consumer, record) == record.offset() + 1
    assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()["count"] == 1
    assert connection.execute("SELECT count(*) FROM mock_provider_acceptances").fetchone()["count"] == 0


def test_independent_consumer_groups_each_receive_every_event(client, event, connection, kafka):
    other = {**event, "event_id": "evt_other", "appointment_id": "appt_other"}
    for payload in (event, other):
        assert client.post("/events", json=payload).status_code == 201
        assert publish_one(connection, kafka.publisher)
    for module in (notifications, analytics):
        consumer = kafka.consumer(f"{module.GROUP_ID}-{uuid4().hex}")
        keys = set()
        for _ in range(2):
            record = kafka.read(consumer)
            keys.add(record.key())
            worker.process_record(consumer, record, module.process_event)
        assert keys == {b"appt_456", b"appt_other"}
    assert connection.execute("SELECT count(*) FROM analytics_events").fetchone()["count"] == 2
    intents = connection.execute("SELECT * FROM notifications").fetchall()
    assert len(intents) == 2
    assert all(row["status"] == "PENDING" for row in intents)
    assert connection.execute("SELECT count(*) FROM mock_provider_acceptances").fetchone()["count"] == 0


@pytest.mark.parametrize("handler,table", [
    (notifications.process_event, "notifications"),
    (analytics.process_event, "analytics_events"),
])
def test_consumer_restart_after_database_commit_before_offset_commit(
    client, event, connection, kafka, handler, table
):
    assert client.post("/events", json=event).status_code == 201
    assert publish_one(connection, kafka.publisher)
    group = f"restart-{uuid4().hex}"
    consumer = kafka.consumer(group)
    record = kafka.read(consumer)

    class CrashBeforeCommit:
        def commit(self, **kwargs):
            raise RuntimeError("injected crash before offset commit")
    with pytest.raises(RuntimeError, match="before offset"):
        worker.process_record(CrashBeforeCommit(), record, handler)
    assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()["count"] == 1
    assert kafka.offset(consumer, record) == OFFSET_INVALID
    kafka.close(consumer)
    consumer = kafka.consumer(group)
    replay = kafka.read(consumer)
    assert (replay.partition(), replay.offset()) == (record.partition(), record.offset())
    worker.process_record(consumer, replay, handler)
    assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()["count"] == 1
    assert kafka.offset(consumer, replay) == replay.offset() + 1
    kafka.close(consumer)
    consumer = kafka.consumer(group)
    assert kafka.offset(consumer, replay) == replay.offset() + 1
    other = {**event, "event_id": "evt_other", "appointment_id": "appt_other"}
    assert client.post("/events", json=other).status_code == 201
    assert publish_one(connection, kafka.publisher)
    next_record = kafka.read(consumer)
    assert json.loads(next_record.value())["event_id"] == "evt_other"
    worker.process_record(consumer, next_record, handler)


def test_repeated_cancellation_only_warrants_one_cancellation_intent(client, event, connection, kafka):
    for payload in (event, *[
        {**event, "event_id": f"evt_cancel_{v}", "event_type": "APPOINTMENT_CANCELLED", "event_version": v}
        for v in (2, 3)
    ]):
        assert client.post("/events", json=payload).status_code == 201
        assert publish_one(connection, kafka.publisher)
    for module in (notifications, analytics):
        consumer = kafka.consumer()
        for _ in range(3):
            worker.process_record(consumer, kafka.read(consumer), module.process_event)
    intents = connection.execute("SELECT * FROM notifications").fetchall()
    assert {row["event_id"] for row in intents} == {"evt_001", "evt_cancel_2"}
    assert connection.execute("SELECT count(*) FROM analytics_events").fetchone()["count"] == 3
    assert connection.execute("SELECT count(*) FROM mock_provider_acceptances").fetchone()["count"] == 0


@pytest.mark.parametrize("handler,table", [
    (notifications.process_event, "notifications"),
    (analytics.process_event, "analytics_events"),
])
def test_database_failure_retries_then_stops_without_committing(
    client, event, connection, kafka, monkeypatch, handler, table
):
    assert client.post("/events", json=event).status_code == 201
    assert publish_one(connection, kafka.publisher)
    connection.execute(f"ALTER TABLE {table} ADD CONSTRAINT injected_failure CHECK (event_id <> 'evt_001')")
    consumer = kafka.consumer()
    record = kafka.read(consumer)
    sleeps = []
    monkeypatch.setattr(worker, "sleep", sleeps.append)
    with pytest.raises(psycopg.errors.CheckViolation):
        worker.process_record(consumer, record, handler)
    assert sleeps == [1, 2]
    assert kafka.offset(consumer, record) == OFFSET_INVALID
    assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()["count"] == 0
    connection.execute(f"ALTER TABLE {table} DROP CONSTRAINT injected_failure")
    worker.process_record(consumer, record, handler)
    assert kafka.offset(consumer, record) == record.offset() + 1


@pytest.mark.parametrize("invalid", ["json", "key", "payload"])
def test_invalid_record_never_commits_and_reappears_after_restart(
    client, event, connection, kafka, invalid
):
    assert client.post("/events", json=event).status_code == 201
    value = connection.execute("SELECT payload FROM outbox").fetchone()["payload"]
    key = event["appointment_id"].encode()
    if invalid == "json":
        value = {}
    elif invalid == "key":
        key = b"wrong"
    else:
        value["patient_id"] = "wrong"
    producer = kafka.publisher.producer
    producer.produce(kafka.topic, key=key, value=json.dumps(value).encode())
    assert producer.flush(6) == 0
    group = f"invalid-{uuid4().hex}"
    consumer = kafka.consumer(group)
    record = kafka.read(consumer)
    with pytest.raises(ValueError):
        worker.process_record(consumer, record, notifications.process_event)
    assert kafka.offset(consumer, record) == OFFSET_INVALID
    kafka.close(consumer)
    consumer = kafka.consumer(group)
    retry = kafka.read(consumer)
    assert (retry.partition(), retry.offset()) == (record.partition(), record.offset())
    assert connection.execute("SELECT count(*) FROM notifications").fetchone()["count"] == 0


def test_phase2_migration_preserves_events_without_sending_historical_notices(client, event, connection):
    original = client.post("/events", json=event)
    connection.execute("DROP TABLE notification_deliveries, notifications, mock_provider_acceptances")
    connection.execute("ALTER TABLE events DROP COLUMN notification_type")
    migrate(connection)
    assert client.post("/events", json=event).content == original.content
    assert connection.execute("SELECT notification_type FROM events").fetchone()["notification_type"] is None
    assert connection.execute("SELECT count(*) FROM notifications").fetchone()["count"] == 0


def test_offset_commit_errors_retry_durable_processing_safely(
    client, event, connection, kafka, monkeypatch
):
    assert client.post("/events", json=event).status_code == 201
    assert publish_one(connection, kafka.publisher)
    consumer = kafka.consumer()
    record = kafka.read(consumer)
    calls = []

    class UnreliableCommit:
        def commit(self, **kwargs):
            calls.append(kwargs)
            assert connection.execute("SELECT count(*) FROM analytics_events").fetchone()["count"] == 1
            if len(calls) == 1:
                raise KafkaException("injected commit failure")
            if len(calls) == 2:
                from confluent_kafka import KafkaError
                from types import SimpleNamespace
                return [SimpleNamespace(error=KafkaError(KafkaError._TRANSPORT))]
            return consumer.commit(**kwargs)

    monkeypatch.setattr(worker, "sleep", lambda seconds: None)
    worker.process_record(UnreliableCommit(), record, analytics.process_event)
    assert len(calls) == 3
    assert kafka.offset(consumer, record) == record.offset() + 1
    assert connection.execute("SELECT count(*) FROM analytics_events").fetchone()["count"] == 1
