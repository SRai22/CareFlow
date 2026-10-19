from datetime import datetime, timedelta, timezone

from psycopg.types.json import Jsonb
from temporalio import activity
from temporalio.exceptions import ApplicationError

from app.db import open_connection
from app import mock_provider
from app.workflows.appointment_reminder import ReminderInput


TYPE = "APPOINTMENT_REMINDER"


def provider_key(request):
    return f"appointment-reminder:{request.appointment_id}:g{request.schedule_generation}"


def outcome(row):
    return {
        "appointment_id": row["appointment_id"], "schedule_generation": row["schedule_generation"],
        "status": row["status"], "provider_message_id": str(row["provider_message_id"]) if row["provider_message_id"] else None,
        "invalidated_after_claim": row["claimed_at"] is not None and row["invalidated_at"] is not None,
    }


def claim(request):
    with open_connection() as connection, connection.transaction():
        # Cancellation/rescheduling take this same lock: commit order defines the cutoff.
        appointment = connection.execute(
            "SELECT * FROM appointments WHERE appointment_id = %s FOR UPDATE", (request.appointment_id,),
        ).fetchone()
        event = connection.execute("SELECT * FROM events WHERE event_id = %s", (request.event_id,)).fetchone()
        if (
            appointment is None or event is None
            or event["appointment_id"] != request.appointment_id
            or event["patient_id"] != appointment["patient_id"]
            or event["schedule_generation"] != request.schedule_generation
            or event["event_type"] not in ("APPOINTMENT_SCHEDULED", "APPOINTMENT_RESCHEDULED")
            or event["scheduled_for"] != datetime.fromisoformat(request.scheduled_for)
        ):
            raise ValueError("Reminder input does not match its committed scheduling event")
        key = provider_key(request)
        payload = {
            "appointment_id": request.appointment_id, "patient_id": event["patient_id"],
            "schedule_generation": request.schedule_generation,
            "scheduled_for": event["scheduled_for"].astimezone(timezone.utc).isoformat(), "notification_type": TYPE,
        }
        connection.execute(
            """INSERT INTO notifications(event_id, notification_type, provider_key)
               VALUES (%s, %s, %s) ON CONFLICT(event_id, notification_type) DO NOTHING""",
            (request.event_id, TYPE, key),
        )
        connection.execute(
            """INSERT INTO notification_deliveries
               (appointment_id, schedule_generation, event_id, provider_key, payload)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT(appointment_id, schedule_generation, notification_type) DO NOTHING""",
            (request.appointment_id, request.schedule_generation, request.event_id, key, Jsonb(payload)),
        )
        delivery = connection.execute(
            "SELECT * FROM notification_deliveries WHERE provider_key = %s FOR UPDATE", (key,),
        ).fetchone()
        if delivery["event_id"] != request.event_id or delivery["payload"] != payload:
            raise ValueError("Delivery identity reused with a different scheduling event")
        if delivery["status"] not in ("PENDING", "DISPATCHING"):
            return delivery
        if delivery["status"] == "PENDING":
            if (
                appointment["status"] != "SCHEDULED"
                or appointment["schedule_generation"] != request.schedule_generation
                or appointment["scheduled_for"] != event["scheduled_for"]
            ):
                connection.execute(
                    """UPDATE notifications SET status = 'SUPPRESSED'
                       WHERE event_id = %s AND notification_type = %s""", (request.event_id, TYPE),
                )
                return connection.execute(
                    """UPDATE notification_deliveries SET status = 'SUPPRESSED', completed_at = clock_timestamp(),
                           last_error = 'Appointment cancelled or schedule superseded'
                       WHERE provider_key = %s RETURNING *""", (key,),
                ).fetchone()
            if appointment["scheduled_for"] - timedelta(hours=24) > datetime.now(timezone.utc):
                raise RuntimeError("Reminder is not due yet")
        # A DISPATCHING retry continues the already-authorized logical send, even after cancellation.
        return connection.execute(
            """UPDATE notification_deliveries SET status = 'DISPATCHING',
                   claimed_at = COALESCE(claimed_at, clock_timestamp()),
                   attempt_count = attempt_count + 1, last_attempt_at = clock_timestamp()
               WHERE provider_key = %s RETURNING *""", (key,),
        ).fetchone()


def mark_sent(key, message_id):
    with open_connection() as connection, connection.transaction():
        row = connection.execute(
            """UPDATE notification_deliveries SET status = 'SENT', provider_message_id = %s,
                   completed_at = clock_timestamp(), last_error = NULL
               WHERE provider_key = %s RETURNING *""", (message_id, key),
        ).fetchone()
        connection.execute(
            """UPDATE notifications SET status = 'SENT', provider_message_id = %s, sent_at = clock_timestamp()
               WHERE event_id = %s AND notification_type = %s""", (message_id, row["event_id"], TYPE),
        )
        return row


def deliver(request):
    row = claim(request)
    if row["status"] != "DISPATCHING":
        return outcome(row)
    try:
        message_id = mock_provider.send_payload(row["provider_key"], row["payload"])
        return outcome(mark_sent(row["provider_key"], message_id))
    except Exception as exc:
        with open_connection() as connection:
            connection.execute(
                """UPDATE notification_deliveries SET last_error = %s
                   WHERE provider_key = %s AND status = 'DISPATCHING'""", (str(exc)[:2000], row["provider_key"]),
            )
        raise


@activity.defn
def deliver_reminder(request: ReminderInput) -> dict:
    try:
        return deliver(request)
    except ValueError as exc:
        raise ApplicationError(str(exc), non_retryable=True) from exc


@activity.defn
def finish_uncertain_delivery(request: ReminderInput) -> dict:
    with open_connection() as connection, connection.transaction():
        row = connection.execute(
            "SELECT * FROM notification_deliveries WHERE provider_key = %s FOR UPDATE", (provider_key(request),),
        ).fetchone()
        if row is None or row["event_id"] != request.event_id:
            return {"appointment_id": request.appointment_id, "status": "FAILED"}
        if row["status"] in ("PENDING", "DISPATCHING"):
            row = connection.execute(
                """UPDATE notification_deliveries SET status = 'UNCERTAIN', completed_at = clock_timestamp(),
                       last_error = COALESCE(last_error, 'Activity retry budget exhausted or timed out')
                   WHERE provider_key = %s RETURNING *""", (provider_key(request),),
            ).fetchone()
            connection.execute(
                "UPDATE notifications SET status = 'UNKNOWN' WHERE event_id = %s AND notification_type = %s",
                (request.event_id, TYPE),
            )
        return outcome(row)
