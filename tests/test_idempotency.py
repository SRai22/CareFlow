from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from app.migrate_idempotency import migrate


def snapshot(connection):
    return (
        connection.execute("SELECT * FROM events ORDER BY event_id").fetchall(),
        connection.execute("SELECT * FROM appointments ORDER BY appointment_id").fetchall(),
    )


@pytest.mark.parametrize("event_type", ["APPOINTMENT_SCHEDULED", "APPOINTMENT_CANCELLED"])
def test_matching_retry_replays_stored_outcome_without_changes(
    client, event, connection, event_type
):
    payload = event
    if event_type == "APPOINTMENT_CANCELLED":
        assert client.post("/events", json=event).status_code == 201
        payload = {**event, "event_id": "evt_cancel", "event_type": event_type}

    original = client.post("/events", json=payload)
    assert original.status_code == 201
    before = snapshot(connection)
    stored = connection.execute(
        "SELECT * FROM events WHERE event_id = %s", (payload["event_id"],)
    ).fetchone()
    assert len(stored["request_hash"]) == 64
    assert stored["response_status"] == original.status_code
    assert stored["response_body"] == original.json()

    for _ in range(2):
        retry = client.post("/events", json=payload)
        assert retry.status_code == original.status_code
        assert retry.content == original.content
        assert retry.headers["location"] == original.headers["location"]
        assert snapshot(connection) == before


@pytest.mark.parametrize(
    "changes",
    [
        {"appointment_id": "appt_other"},
        {"patient_id": "patient_other"},
        {"event_type": "APPOINTMENT_CANCELLED"},
        {"event_version": 2},
        {"occurred_at": "2026-10-17T11:00:00Z"},
    ],
)
def test_changed_payload_conflicts_without_changes(client, event, connection, changes):
    assert client.post("/events", json=event).status_code == 201
    before = snapshot(connection)
    response = client.post("/events", json={**event, **changes})
    assert response.status_code == 409
    assert response.json()["detail"] == "Event ID already exists with a different payload"
    assert snapshot(connection) == before


def test_equivalent_timestamp_and_json_order_replay_original(client, event, connection):
    original = client.post("/events", json=event)
    assert original.status_code == 201
    before = snapshot(connection)
    equivalent = dict(reversed(list(event.items())))
    equivalent["occurred_at"] = "2026-10-17T15:30:00.000000+05:30"
    retry = client.post("/events", json=equivalent)
    assert retry.status_code == 201
    assert retry.content == original.content
    assert snapshot(connection) == before


def test_old_schedule_retry_does_not_undo_later_cancellation(client, event, connection):
    original = client.post("/events", json=event)
    assert original.status_code == 201
    assert client.post(
        "/events",
        json={**event, "event_id": "evt_cancel", "event_type": "APPOINTMENT_CANCELLED"},
    ).status_code == 201
    before = snapshot(connection)
    retry = client.post("/events", json=event)
    assert retry.status_code == 201
    assert retry.content == original.content
    assert snapshot(connection) == before
    assert client.get("/appointments/appt_456").json()["status"] == "CANCELLED"


def test_failed_request_has_no_outcome_and_can_succeed_later(client, event, connection):
    cancellation = {**event, "event_id": "evt_cancel", "event_type": "APPOINTMENT_CANCELLED"}
    assert client.post("/events", json=cancellation).status_code == 404
    assert snapshot(connection) == ([], [])
    assert client.post("/events", json=event).status_code == 201
    assert client.post("/events", json=cancellation).status_code == 201


@pytest.mark.parametrize("different_payload", [False, True])
def test_concurrent_retries_commit_one_event_and_appointment(
    client, event, connection, different_payload
):
    other = {**event, "appointment_id": "appt_other"} if different_payload else event
    ready = Barrier(2)

    def send(payload):
        ready.wait(timeout=10)
        return client.post("/events", json=payload)

    with ThreadPoolExecutor(max_workers=2) as workers:
        futures = [workers.submit(send, payload) for payload in (event, other)]
        responses = [future.result(timeout=15) for future in futures]

    assert sorted(response.status_code for response in responses) == (
        [201, 409] if different_payload else [201, 201]
    )
    events, appointments = snapshot(connection)
    assert len(events) == len(appointments) == 1
    successful = next(response for response in responses if response.status_code == 201)
    assert events[0]["response_body"] == successful.json()
    assert appointments[0]["appointment_id"] == successful.json()["appointment_id"]
    if not different_payload:
        assert responses[0].content == responses[1].content


def test_migration_backfills_existing_success_for_replay(client, event, connection):
    original = client.post("/events", json=event)
    assert original.status_code == 201
    appointment_before = client.get("/appointments/appt_456").json()
    # Recreate the previous schema while retaining its committed event and appointment.
    connection.execute("""
        ALTER TABLE events
            DROP COLUMN request_hash,
            DROP COLUMN response_status,
            DROP COLUMN response_body
    """)
    migrate(connection)

    retry = client.post("/events", json=event)
    assert retry.status_code == 201
    assert retry.json() == original.json()
    assert client.get("/appointments/appt_456").json() == appointment_before
    stored = connection.execute("SELECT * FROM events").fetchone()
    assert len(stored["request_hash"]) == 64
    assert stored["response_status"] == 201
    assert stored["response_body"] == original.json()
