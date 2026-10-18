import psycopg
import pytest
from psycopg.errors import CheckViolation, UniqueViolation
from psycopg.rows import dict_row

from app.schemas import EventCreate
from app.transactions import process_event


@pytest.fixture
def observer(database_url):
    with psycopg.connect(database_url, autocommit=True, row_factory=dict_row) as conn:
        yield conn


@pytest.fixture
def scheduled_appointment(connection, event):
    process_event(EventCreate(**event), connection)


@pytest.fixture
def cancellation(event):
    return EventCreate(
        **{
            **event,
            "event_id": "evt_cancel",
            "event_type": "APPOINTMENT_CANCELLED",
            "event_version": 2,
            "occurred_at": "2026-10-17T11:00:00Z",
        }
    )


def snapshot(observer):
    return (
        observer.execute("SELECT * FROM events ORDER BY event_id").fetchall(),
        observer.execute("SELECT * FROM appointments ORDER BY appointment_id").fetchall(),
    )


def test_successful_cancellation_commits_both_changes(
    connection, observer, scheduled_appointment, cancellation
):
    before = snapshot(observer)
    stored = process_event(cancellation, connection)
    events, appointments = snapshot(observer)

    assert len(events) == 2
    assert stored in events
    assert stored["event_type"] == "APPOINTMENT_CANCELLED"
    assert appointments[0]["status"] == "CANCELLED"
    assert appointments[0]["last_event_id"] == cancellation.event_id
    assert appointments[0]["updated_at"] > before[1][0]["updated_at"]


def test_unknown_appointment_raises_lookup_error_and_rolls_back(
    connection, observer, cancellation
):
    before = snapshot(observer)
    with pytest.raises(LookupError, match="Appointment not found"):
        process_event(cancellation, connection)
    assert snapshot(observer) == before


def test_duplicate_event_id_raises_unique_violation_without_changes(
    connection, observer, scheduled_appointment, cancellation, event
):
    before = snapshot(observer)
    duplicate = cancellation.model_copy(update={"event_id": event["event_id"]})
    with pytest.raises(UniqueViolation) as error:
        process_event(duplicate, connection)
    assert error.value.diag.constraint_name == "events_pkey"
    assert snapshot(observer) == before


def test_wrong_patient_rolls_back_without_changes(
    connection, observer, scheduled_appointment, cancellation
):
    before = snapshot(observer)
    wrong_patient = cancellation.model_copy(update={"patient_id": "patient_other"})
    with pytest.raises(LookupError, match="another patient"):
        process_event(wrong_patient, connection)
    assert snapshot(observer) == before


def test_constraint_failure_rolls_back_event_and_appointment_update(
    connection, observer, scheduled_appointment, cancellation
):
    # Fail the second write using a real constraint in this test's isolated schema.
    connection.execute("""
        ALTER TABLE appointments ADD CONSTRAINT test_reject_cancellation
        CHECK (status <> 'CANCELLED')
    """)
    before = snapshot(observer)
    with pytest.raises(CheckViolation) as error:
        process_event(cancellation, connection)
    assert error.value.diag.constraint_name == "test_reject_cancellation"
    assert snapshot(observer) == before
