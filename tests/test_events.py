from datetime import datetime

import pytest


def test_schedule_persists_event_and_appointment(client, event):
    response = client.post("/events", json=event)
    assert response.status_code == 201
    assert response.headers["location"] == "/events/evt_001"
    stored = response.json()
    assert {key: stored[key] for key in event} == event
    assert datetime.fromisoformat(stored["received_at"]).tzinfo is not None
    assert client.get("/events/evt_001").json() == stored

    appointment = client.get("/appointments/appt_456")
    assert appointment.status_code == 200
    assert appointment.json() == {
        "appointment_id": "appt_456",
        "patient_id": "patient_123",
        "status": "SCHEDULED",
        "last_event_id": "evt_001",
        "updated_at": appointment.json()["updated_at"],
    }


def test_cancellation_updates_state_and_preserves_history(client, event, connection):
    assert client.post("/events", json=event).status_code == 201
    before = client.get("/appointments/appt_456").json()
    cancellation = {
        **event,
        "event_id": "evt_002",
        "event_type": "APPOINTMENT_CANCELLED",
        "event_version": 2,
        "occurred_at": "2026-10-17T11:00:00Z",
    }
    assert client.post("/events", json=cancellation).status_code == 201
    appointment = client.get("/appointments/appt_456").json()
    assert appointment["status"] == "CANCELLED"
    assert appointment["last_event_id"] == "evt_002"
    assert appointment["updated_at"] != before["updated_at"]
    assert client.get("/events/evt_001").json()["event_type"] == event["event_type"]
    assert connection.execute("SELECT count(*) FROM events").fetchone()["count"] == 2


def test_duplicate_id_with_different_content_conflicts_without_changing_state(
    client, event, connection
):
    assert client.post("/events", json=event).status_code == 201
    before = client.get("/appointments/appt_456").json()
    response = client.post(
        "/events", json={**event, "event_type": "APPOINTMENT_CANCELLED"}
    )
    assert response.status_code == 409
    assert response.json()["detail"] == "Event ID already exists with a different payload"
    assert client.get("/appointments/appt_456").json() == before
    assert connection.execute("SELECT count(*) FROM events").fetchone()["count"] == 1


@pytest.mark.parametrize(
    ("event_type", "expected_status"),
    [("APPOINTMENT_SCHEDULED", 409), ("APPOINTMENT_CANCELLED", 404)],
)
def test_patient_conflict_rolls_back_event(client, event, event_type, expected_status):
    assert client.post("/events", json=event).status_code == 201
    before = client.get("/appointments/appt_456").json()
    response = client.post(
        "/events",
        json={
            **event,
            "event_id": "evt_wrong",
            "patient_id": "patient_other",
            "event_type": event_type,
        },
    )
    assert response.status_code == expected_status
    assert client.get("/events/evt_wrong").status_code == 404
    assert client.get("/appointments/appt_456").json() == before


def test_cancellation_of_missing_appointment_rolls_back_event(client, event, connection):
    cancellation = {**event, "event_type": "APPOINTMENT_CANCELLED"}
    response = client.post("/events", json=cancellation)
    assert response.status_code == 404
    assert response.json()["detail"] == "Appointment not found for the supplied patient"
    assert client.get("/events/evt_001").status_code == 404
    assert client.get("/appointments/appt_456").status_code == 404
    assert connection.execute("SELECT count(*) FROM events").fetchone()["count"] == 0
    assert connection.execute("SELECT count(*) FROM appointments").fetchone()["count"] == 0


@pytest.mark.parametrize("event_type", ["APPOINTMENT_SCHEDULED", "APPOINTMENT_CANCELLED"])
def test_database_failure_rolls_back_both_writes(client, event, connection, event_type):
    before = None
    if event_type == "APPOINTMENT_CANCELLED":
        assert client.post("/events", json=event).status_code == 201
        before = client.get("/appointments/appt_456").json()

    connection.execute("""
        CREATE FUNCTION fail_appointment_write() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'Injected appointment write failure';
        END;
        $$;
        CREATE TRIGGER fail_write BEFORE INSERT OR UPDATE ON appointments
        FOR EACH ROW EXECUTE FUNCTION fail_appointment_write();
    """)
    failed_event = {**event, "event_id": "evt_failed", "event_type": event_type}
    assert client.post("/events", json=failed_event).status_code == 500
    assert client.get("/events/evt_failed").status_code == 404
    if before is None:
        assert client.get("/appointments/appt_456").status_code == 404
    else:
        assert client.get("/appointments/appt_456").json() == before
        assert client.get("/events/evt_001").status_code == 200


@pytest.mark.parametrize("path", ["/events/missing", "/appointments/missing"])
def test_missing_resource_returns_404(client, path):
    assert client.get(path).status_code == 404


@pytest.mark.parametrize(
    "changes",
    [
        {"event_id": ""},
        {"event_id": " "},
        {"patient_id": "x" * 129},
        {"appointment_id": "bad/id"},
        {"event_type": "UNKNOWN"},
        {"event_version": 0},
        {"event_version": -1},
        {"event_version": True},
        {"event_version": "1"},
        {"event_version": 2_147_483_648},
        {"occurred_at": "2026-10-17T10:00:00"},
        {"occurred_at": "not-a-date"},
        {"extra_field": "unexpected"},
    ],
)
def test_invalid_event_is_not_stored(client, event, connection, changes):
    assert client.post("/events", json={**event, **changes}).status_code == 422
    assert connection.execute("SELECT count(*) FROM events").fetchone()["count"] == 0
    assert connection.execute("SELECT count(*) FROM appointments").fetchone()["count"] == 0


def test_missing_required_field(client, event):
    del event["patient_id"]
    assert client.post("/events", json=event).status_code == 422


def test_offset_timestamp_preserves_instant(client, event):
    event["occurred_at"] = "2026-10-17T15:30:00+05:30"
    response = client.post("/events", json=event)
    assert response.status_code == 201
    assert datetime.fromisoformat(response.json()["occurred_at"]) == datetime.fromisoformat(
        "2026-10-17T10:00:00Z"
    )


@pytest.mark.parametrize("cancel_first", [False, True])
def test_scheduling_existing_appointment_conflicts_and_rolls_back(
    client, event, connection, cancel_first
):
    assert client.post("/events", json=event).status_code == 201
    if cancel_first:
        cancellation = {
            **event,
            "event_id": "evt_cancel",
            "event_type": "APPOINTMENT_CANCELLED",
            "event_version": 3,
        }
        assert client.post("/events", json=cancellation).status_code == 201
    before = client.get("/appointments/appt_456").json()
    reschedule = {
        **event,
        "event_id": "evt_reschedule",
        "event_version": 2,
        "occurred_at": "2026-10-17T10:30:00Z",
    }
    response = client.post("/events", json=reschedule)
    assert response.status_code == 409
    assert response.json()["detail"] == "Appointment already exists"
    assert client.get("/events/evt_reschedule").status_code == 404
    assert client.get("/appointments/appt_456").json() == before
    assert connection.execute("SELECT count(*) FROM events").fetchone()["count"] == (
        2 if cancel_first else 1
    )


def test_repeated_cancellation_with_new_event_id_is_accepted(client, event, connection):
    assert client.post("/events", json=event).status_code == 201
    for version in (2, 3):
        cancellation = {
            **event,
            "event_id": f"evt_cancel_{version}",
            "event_type": "APPOINTMENT_CANCELLED",
            "event_version": version,
        }
        assert client.post("/events", json=cancellation).status_code == 201
    appointment = client.get("/appointments/appt_456").json()
    assert appointment["status"] == "CANCELLED"
    assert appointment["last_event_id"] == "evt_cancel_3"
    assert connection.execute("SELECT count(*) FROM events").fetchone()["count"] == 3


def test_unrelated_unique_violation_is_not_mapped_to_conflict(client, event, connection):
    assert client.post("/events", json=event).status_code == 201
    before = client.get("/appointments/appt_456").json()
    connection.execute(
        "ALTER TABLE appointments ADD CONSTRAINT test_unique_patient UNIQUE (patient_id)"
    )
    response = client.post(
        "/events",
        json={**event, "event_id": "evt_failed", "appointment_id": "appt_other"},
    )
    assert response.status_code == 500
    assert response.text == "Internal Server Error"
    assert client.get("/events/evt_failed").status_code == 404
    assert client.get("/appointments/appt_other").status_code == 404
    assert client.get("/appointments/appt_456").json() == before
