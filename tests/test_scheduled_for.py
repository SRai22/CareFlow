from datetime import datetime, timedelta, timezone
import hashlib
import json
from types import SimpleNamespace

from psycopg.types.json import Jsonb
import pytest

from app import transactions
from app.idempotency import request_hash
from app.migrate_scheduled_for import migrate
from app.schemas import EventCreate


@pytest.mark.parametrize("scheduled_for", [
    None, "2020-01-01T00:00:00Z", "not-a-date", "2040-01-01T00:00:00",
])
def test_invalid_new_appointment_time_rolls_back_every_write(client, event, connection, scheduled_for):
    response = client.post("/events", json={**event, "scheduled_for": scheduled_for})
    assert response.status_code == 422
    for table in ("events", "appointments", "outbox"):
        assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()["count"] == 0


def test_missing_time_is_rejected_for_new_schedule_but_not_cancellation(client, event, connection):
    without_time = {key: value for key, value in event.items() if key != "scheduled_for"}
    assert client.post("/events", json=without_time).status_code == 422
    assert client.post("/events", json=event).status_code == 201
    cancellation = {**without_time, "event_id": "evt_cancel", "event_type": "APPOINTMENT_CANCELLED"}
    assert client.post("/events", json=cancellation).status_code == 201
    assert client.get("/appointments/appt_456").json()["scheduled_for"] == event["scheduled_for"]


def test_time_is_normalized_persisted_fingerprinted_and_immutable(client, event, connection):
    instant = datetime.fromisoformat(event["scheduled_for"])
    offset_form = instant.astimezone(timezone(timedelta(hours=5, minutes=30))).isoformat()
    original = client.post("/events", json={**event, "scheduled_for": offset_form})
    assert original.status_code == 201
    assert original.json()["scheduled_for"] == event["scheduled_for"]
    outbox = connection.execute("SELECT * FROM outbox").fetchone()
    assert outbox["payload"]["scheduled_for"] == event["scheduled_for"]
    assert client.get("/events/evt_001").json() == original.json()
    assert client.post("/events", json=event).content == original.content
    assert client.post("/events", json={**event, "scheduled_for": (instant + timedelta(hours=1)).isoformat()}).status_code == 409
    assert client.post("/events", json={**event, "event_id": "evt_cancel", "event_type": "APPOINTMENT_CANCELLED"}).status_code == 201
    assert connection.execute("SELECT * FROM outbox WHERE event_id = 'evt_001'").fetchone() == outbox


def test_retry_after_appointment_time_passes_replays_original(client, event, connection, monkeypatch):
    original = client.post("/events", json=event)
    assert original.status_code == 201
    later = datetime.fromisoformat(event["scheduled_for"]) + timedelta(days=1)
    monkeypatch.setattr(transactions, "datetime", SimpleNamespace(now=lambda tz: later))
    assert client.post("/events", json=event).content == original.content
    assert client.get("/events/evt_001").json() == original.json()
    new_event = {**event, "event_id": "evt_late", "appointment_id": "appt_late"}
    assert client.post("/events", json=new_event).status_code == 422
    assert connection.execute("SELECT count(*) FROM outbox").fetchone()["count"] == 1
    # Stream decoding is not new-request validation: delayed records remain processable.
    assert EventCreate(**event).scheduled_for < later


def test_additive_migration_preserves_legacy_hash_response_payload_and_retry(client, event, connection):
    original = client.post("/events", json=event)
    assert original.status_code == 201
    legacy = {key: value for key, value in event.items() if key != "scheduled_for"}
    body = {key: value for key, value in original.json().items() if key != "scheduled_for"}
    old_hash = hashlib.sha256(json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    connection.execute("UPDATE events SET request_hash = %s, response_body = %s", (old_hash, Jsonb(body)))
    connection.execute("UPDATE outbox SET payload = %s", (Jsonb(legacy),))
    connection.execute("ALTER TABLE events DROP COLUMN scheduled_for")
    connection.execute("ALTER TABLE appointments DROP COLUMN scheduled_for")
    before = connection.execute("SELECT * FROM outbox").fetchall()
    migrate(connection)
    assert connection.execute("SELECT scheduled_for FROM appointments").fetchone()["scheduled_for"] is None
    assert connection.execute("SELECT scheduled_for FROM events").fetchone()["scheduled_for"] is None
    assert request_hash(EventCreate(**legacy)) == old_hash
    retry = client.post("/events", json=legacy)
    assert retry.status_code == 201
    assert retry.json() == body
    assert client.get("/events/evt_001").json() == body
    assert connection.execute("SELECT * FROM outbox").fetchall() == before
