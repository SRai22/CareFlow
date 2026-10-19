from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from psycopg.rows import dict_row

from app.kafka_producer import PublishError
from app.migrate_outbox import migrate
from app.outbox_publisher import publish_one
from app.schemas import EventCreate


def rows(connection):
    return connection.execute("SELECT * FROM outbox ORDER BY created_at, event_id").fetchall()


def test_outbox_is_atomic_and_retries_do_not_add_rows(client, event, connection):
    assert client.post("/events", json=event).status_code == 201
    original = rows(connection)
    assert len(original) == 1
    assert original[0]["payload"] == {**EventCreate(**event).model_dump(mode="json"), "schedule_generation": 1}
    assert original[0]["published_at"] is None
    assert client.post("/events", json=event).status_code == 201
    assert client.post("/events", json={**event, "patient_id": "wrong"}).status_code == 409
    assert rows(connection) == original
    cancellation = {**event, "event_id": "evt_cancel", "event_type": "APPOINTMENT_CANCELLED"}
    assert client.post("/events", json=cancellation).status_code == 201
    cancelled = rows(connection)
    assert client.post("/events", json=cancellation).status_code == 201
    assert rows(connection) == cancelled


@pytest.mark.parametrize("failure", ["unknown", "wrong_patient", "constraint", "existing"])
def test_failed_transaction_does_not_enqueue(client, event, connection, failure):
    if failure != "unknown":
        assert client.post("/events", json=event).status_code == 201
    before = rows(connection)
    payload = {**event, "event_id": "evt_fail", "event_type": "APPOINTMENT_CANCELLED"}
    expected = 404
    if failure == "wrong_patient":
        payload["patient_id"] = "wrong"
    elif failure == "constraint":
        connection.execute("ALTER TABLE appointments ADD CHECK (status <> 'CANCELLED')")
        expected = 500
    elif failure == "existing":
        payload["event_type"] = "APPOINTMENT_SCHEDULED"
        expected = 409
    assert client.post("/events", json=payload).status_code == expected
    assert rows(connection) == before
    assert connection.execute("SELECT * FROM events WHERE event_id = 'evt_fail'").fetchone() is None


def test_distinct_cancellations_each_enqueue_immutable_payload(client, event, connection):
    for payload in (
        event,
        {**event, "event_id": "evt_cancel_1", "event_type": "APPOINTMENT_CANCELLED"},
        {**event, "event_id": "evt_cancel_2", "event_type": "APPOINTMENT_CANCELLED"},
    ):
        assert client.post("/events", json=payload).status_code == 201
    pending = rows(connection)
    assert len(pending) == 3
    assert pending[0]["payload"]["event_type"] == "APPOINTMENT_SCHEDULED"
    assert {row["event_id"] for row in pending} == {"evt_001", "evt_cancel_1", "evt_cancel_2"}


def test_publication_acknowledgment_marks_row(client, event, connection, publisher):
    assert client.post("/events", json=event).status_code == 201
    assert publish_one(connection, publisher)
    assert rows(connection)[0]["published_at"] is not None
    assert publisher.events[0]["appointment_id"] == event["appointment_id"]
    assert not publish_one(connection, publisher)
    assert len(publisher.events) == 1


def test_publish_failure_rolls_back_and_next_attempt_recovers(
    client, event, connection, publisher, monkeypatch
):
    assert client.post("/events", json=event).status_code == 201
    def fail(record):
        raise PublishError("injected failure")
    with monkeypatch.context() as patch:
        patch.setattr(publisher, "publish", fail)
        with pytest.raises(PublishError):
            publish_one(connection, publisher)
    assert rows(connection)[0]["published_at"] is None
    assert publish_one(connection, publisher)


def test_crash_before_publication_releases_claim(client, event, connection, database_url, publisher):
    assert client.post("/events", json=event).status_code == 201
    # Closing a connection with an open transaction simulates a disconnected/crashed worker.
    crashed = psycopg.connect(database_url, autocommit=True, row_factory=dict_row)
    crashed.execute("BEGIN")
    crashed.execute("SELECT * FROM outbox FOR UPDATE SKIP LOCKED").fetchone()
    crashed.close()
    assert publish_one(connection, publisher)
    assert len(publisher.events) == 1


def test_two_workers_cannot_claim_same_row(client, event, connection, database_url, publisher):
    assert client.post("/events", json=event).status_code == 201
    with connection.transaction():
        connection.execute("SELECT * FROM outbox FOR UPDATE SKIP LOCKED").fetchone()
        def competing_worker():
            with psycopg.connect(database_url, autocommit=True, row_factory=dict_row) as other:
                return publish_one(other, publisher)
        with ThreadPoolExecutor(max_workers=1) as workers:
            assert workers.submit(competing_worker).result(timeout=5) is False
        assert publisher.events == []
    assert publish_one(connection, publisher)


def test_migration_preserves_historical_events_without_requeue(client, event, connection):
    original = client.post("/events", json=event)
    connection.execute("DROP TABLE outbox, analytics_events")
    migrate(connection)
    assert client.post("/events", json=event).content == original.content
    assert rows(connection) == []
    assert client.post("/events", json={**event, "event_id": "evt_new", "appointment_id": "appt_new"}).status_code == 201
    assert len(rows(connection)) == 1
