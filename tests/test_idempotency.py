from concurrent.futures import ThreadPoolExecutor
from time import monotonic, sleep

import psycopg
import pytest
from psycopg.rows import dict_row

from app.migrate_idempotency import migrate
from app.schemas import EventCreate
from app.transactions import schedule_appointment


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
def test_duplicate_waits_for_commit_then_rolls_back_before_replay(
    client, event, connection, database_url, different_payload
):
    other = {**event, "appointment_id": "appt_other"} if different_payload else event
    with ThreadPoolExecutor(max_workers=1) as worker:
        with psycopg.connect(database_url, autocommit=True, row_factory=dict_row) as observer:
            # An outer transaction keeps A's inserts uncommitted after its savepoint exits.
            with connection.transaction():
                original = schedule_appointment(EventCreate(**event), connection)
                pending = worker.submit(client.post, "/events", json=other)

                deadline = monotonic() + 5
                while monotonic() < deadline:
                    blocked = observer.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM pg_stat_activity
                            WHERE %s = ANY(pg_blocking_pids(pid))
                        ) AS blocked
                        """,
                        (connection.info.backend_pid,),
                    ).fetchone()["blocked"]
                    if blocked:
                        break
                    sleep(0.01)
                else:
                    pytest.fail("Worker B did not wait on Worker A's uncommitted insert")

                assert not pending.done()
                assert snapshot(observer) == ([], [])

            # A commits here; B must roll back its UniqueViolation before its SELECT.
            response = pending.result(timeout=10)

    assert response.status_code == (409 if different_payload else 201)
    events, appointments = snapshot(connection)
    assert len(events) == len(appointments) == 1
    assert events == [original]
    assert appointments[0]["appointment_id"] == event["appointment_id"]
    if different_payload:
        assert response.json()["detail"] == "Event ID already exists with a different payload"
    else:
        assert response.json() == original["response_body"]
        assert response.headers["location"] == f"/events/{event['event_id']}"


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
