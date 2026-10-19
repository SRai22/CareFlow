from datetime import datetime, timedelta, timezone

import pytest

from app import mock_provider, reminder_delivery
from app.workflows.appointment_reminder import ReminderInput


@pytest.fixture
def due_event(event):
    return {**event, "scheduled_for": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()}


@pytest.fixture
def request_input(client, due_event):
    assert client.post("/events", json=due_event).status_code == 201
    return ReminderInput(
        due_event["appointment_id"], scheduled_for=due_event["scheduled_for"],
        schedule_generation=1, event_id=due_event["event_id"],
    )


def count_provider(connection):
    return connection.execute("SELECT count(*) FROM mock_provider_acceptances").fetchone()["count"]


def cancel(client, event):
    response = client.post("/events", json={**event, "event_id": "evt_cancel", "event_type": "APPOINTMENT_CANCELLED"})
    assert response.status_code == 201
    return response


def test_delivery_reuses_intent_and_provider_key(request_input, connection):
    first = reminder_delivery.deliver(request_input)
    assert first["status"] == "SENT"
    assert reminder_delivery.deliver(request_input) == first
    assert count_provider(connection) == 1
    row = connection.execute("SELECT * FROM notification_deliveries").fetchone()
    assert row["attempt_count"] == 1
    assert row["provider_key"] == f"appointment-reminder:{request_input.appointment_id}:g1"
    intent = connection.execute("SELECT * FROM notifications WHERE notification_type = 'APPOINTMENT_REMINDER'").fetchone()
    assert intent["status"] == "SENT"
    assert intent["provider_message_id"] == row["provider_message_id"]


def test_cancellation_before_claim_suppresses_delivery(client, due_event, request_input, connection):
    cancel(client, due_event)
    assert reminder_delivery.deliver(request_input)["status"] == "SUPPRESSED"
    assert count_provider(connection) == 0
    row = connection.execute("SELECT * FROM notification_deliveries").fetchone()
    assert row["attempt_count"] == 0 and row["claimed_at"] is None
    assert connection.execute("SELECT status FROM notifications WHERE notification_type = 'APPOINTMENT_REMINDER'").fetchone()["status"] == "SUPPRESSED"


def test_reschedule_invalidates_old_generation_and_creates_new_delivery(client, due_event, request_input, connection):
    rescheduled = {**due_event, "event_id": "evt_rescheduled", "event_type": "APPOINTMENT_RESCHEDULED", "scheduled_for": (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()}
    first = client.post("/events", json=rescheduled)
    assert first.status_code == 201
    assert client.post("/events", json=rescheduled).content == first.content
    assert client.get(f'/appointments/{due_event["appointment_id"]}').json()["schedule_generation"] == 2
    assert reminder_delivery.deliver(request_input)["status"] == "SUPPRESSED"
    new_input = ReminderInput(request_input.appointment_id, scheduled_for=rescheduled["scheduled_for"], schedule_generation=2, event_id="evt_rescheduled")
    assert reminder_delivery.deliver(new_input)["status"] == "SENT"
    assert count_provider(connection) == 1
    assert {row["payload"]["schedule_generation"] for row in connection.execute("SELECT payload FROM outbox").fetchall()} == {1, 2}


def test_cancellation_after_claim_keeps_authorized_send_and_records_cutoff(
    client, due_event, request_input, connection, monkeypatch
):
    send = mock_provider.send_payload
    def in_flight(key, payload):
        row = connection.execute("SELECT * FROM notification_deliveries").fetchone()
        assert row["status"] == "DISPATCHING" and row["claimed_at"] is not None
        # The claim has committed and releases its appointment lock before provider I/O.
        cancel(client, due_event)
        return send(key, payload)
    monkeypatch.setattr(mock_provider, "send_payload", in_flight)
    result = reminder_delivery.deliver(request_input)
    assert result["status"] == "SENT" and result["invalidated_after_claim"]
    assert count_provider(connection) == 1
    assert reminder_delivery.deliver(request_input) == result


def test_provider_accepts_then_times_out_and_retry_after_cancel_is_deduplicated(
    client, due_event, request_input, connection, monkeypatch
):
    monkeypatch.setenv("MOCK_PROVIDER_TIMEOUT_AFTER_ACCEPT_ONCE", "1")
    with pytest.raises(mock_provider.ProviderError, match="after durable"):
        reminder_delivery.deliver(request_input)
    accepted = connection.execute("SELECT * FROM mock_provider_acceptances").fetchone()
    assert connection.execute("SELECT status FROM notification_deliveries").fetchone()["status"] == "DISPATCHING"
    cancel(client, due_event)
    result = reminder_delivery.deliver(request_input)
    assert result["status"] == "SENT" and result["invalidated_after_claim"]
    assert result["provider_message_id"] == str(accepted["message_id"])
    assert count_provider(connection) == 1
    assert connection.execute("SELECT attempt_count FROM notification_deliveries").fetchone()["attempt_count"] == 2


def test_crash_after_provider_acceptance_before_recording_success(request_input, connection, monkeypatch):
    with monkeypatch.context() as patch:
        def crash(*args):
            raise RuntimeError("crash after acceptance")
        patch.setattr(reminder_delivery, "mark_sent", crash)
        with pytest.raises(RuntimeError, match="after acceptance"):
            reminder_delivery.deliver(request_input)
    assert count_provider(connection) == 1
    assert reminder_delivery.deliver(request_input)["status"] == "SENT"
    assert count_provider(connection) == 1


def test_retry_exhaustion_preserves_uncertainty_and_late_response_can_resolve(request_input, connection):
    row = reminder_delivery.claim(request_input)
    result = reminder_delivery.finish_uncertain_delivery(request_input)
    assert result["status"] == "UNCERTAIN"
    assert reminder_delivery.deliver(request_input)["status"] == "UNCERTAIN"
    assert count_provider(connection) == 0
    # A request from the already-authorized attempt can complete after its timeout.
    accepted = mock_provider.send_payload(row["provider_key"], row["payload"])
    assert reminder_delivery.mark_sent(row["provider_key"], accepted)["status"] == "SENT"
    assert reminder_delivery.finish_uncertain_delivery(request_input)["status"] == "SENT"


@pytest.mark.parametrize("failure", ["unknown", "wrong_patient", "cancelled", "past_time"])
def test_rejected_reschedule_rolls_back_event_outbox_and_generation(client, due_event, request_input, connection, failure):
    if failure == "cancelled":
        cancel(client, due_event)
    before = connection.execute("SELECT * FROM appointments").fetchall()
    before_outbox = connection.execute("SELECT * FROM outbox ORDER BY event_id").fetchall()
    payload = {**due_event, "event_id": "evt_rescheduled", "event_type": "APPOINTMENT_RESCHEDULED"}
    expected = 404
    if failure == "unknown":
        payload["appointment_id"] = "missing"
    elif failure == "wrong_patient":
        payload["patient_id"] = "wrong"
    elif failure == "cancelled":
        expected = 409
    else:
        payload["scheduled_for"] = "2020-01-01T00:00:00Z"
        expected = 422
    assert client.post("/events", json=payload).status_code == expected
    assert connection.execute("SELECT * FROM appointments").fetchall() == before
    assert connection.execute("SELECT * FROM outbox ORDER BY event_id").fetchall() == before_outbox
    assert connection.execute("SELECT 1 FROM events WHERE event_id = 'evt_rescheduled'").fetchone() is None


def test_concurrent_activity_attempts_share_one_provider_acceptance(request_input, connection, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    rendezvous = Barrier(2)
    send = mock_provider.send_payload
    def concurrent_send(key, payload):
        rendezvous.wait(timeout=5)
        return send(key, payload)
    monkeypatch.setattr(mock_provider, "send_payload", concurrent_send)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(reminder_delivery.deliver, request_input) for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]
    assert results[0]["provider_message_id"] == results[1]["provider_message_id"]
    assert count_provider(connection) == 1
    assert connection.execute("SELECT attempt_count FROM notification_deliveries").fetchone()["attempt_count"] == 2


def test_delivery_migration_preserves_legacy_outcomes_and_payloads(client, due_event, connection):
    from pathlib import Path
    from psycopg.types.json import Jsonb
    from app.migrate_reminder_delivery import migrate
    original = client.post("/events", json=due_event)
    assert original.status_code == 201
    connection.execute("DROP TABLE notification_deliveries, reminder_handoffs")
    connection.execute("DROP TABLE notifications, mock_provider_acceptances")
    connection.execute((Path(__file__).parents[1] / "app/notification_schema.sql").read_text())
    connection.execute("ALTER TABLE appointments DROP COLUMN schedule_generation")
    connection.execute("ALTER TABLE events DROP COLUMN schedule_generation")
    connection.execute("UPDATE outbox SET payload = %s", (Jsonb(due_event),))
    message_id = mock_provider.send_payload("legacy-confirmation", {"legacy": True})
    connection.execute(
        """INSERT INTO notifications(event_id, notification_type, provider_key, status, provider_message_id, sent_at)
           VALUES (%s, 'APPOINTMENT_SCHEDULED', 'legacy-confirmation', 'SENT', %s, clock_timestamp())""",
        (due_event["event_id"], message_id),
    )
    before_intents = connection.execute("SELECT * FROM notifications").fetchall()
    before_outbox = connection.execute("SELECT * FROM outbox").fetchall()
    migrate(connection)
    assert connection.execute("SELECT * FROM notifications").fetchall() == before_intents
    assert connection.execute("SELECT * FROM outbox").fetchall() == before_outbox
    assert connection.execute("SELECT schedule_generation FROM appointments").fetchone()["schedule_generation"] == 1
    assert connection.execute("SELECT schedule_generation FROM events").fetchone()["schedule_generation"] is None
    assert client.post("/events", json=due_event).content == original.content


def test_failure_finalization_cannot_modify_another_event_identity(request_input, connection):
    from dataclasses import replace
    reminder_delivery.claim(request_input)
    invalid = replace(request_input, event_id="wrong_event")
    assert reminder_delivery.finish_uncertain_delivery(invalid)["status"] == "FAILED"
    assert connection.execute("SELECT status FROM notification_deliveries").fetchone()["status"] == "DISPATCHING"
