from typing import Any

import psycopg

from app.schemas import EventCreate


def process_event(
    event: EventCreate, connection: psycopg.Connection[dict[str, Any]]
) -> dict[str, Any]:
    with connection.transaction():
        stored = connection.execute(
            """
            INSERT INTO events (
                event_id, patient_id, appointment_id,
                event_type, event_version, occurred_at
            ) VALUES (
                %(event_id)s, %(patient_id)s, %(appointment_id)s,
                %(event_type)s, %(event_version)s, %(occurred_at)s
            ) RETURNING *
            """,
            event.model_dump(),
        ).fetchone()

        appointment = connection.execute(
            "SELECT * FROM appointments WHERE appointment_id = %s",
            (event.appointment_id,),
        ).fetchone()
        if appointment and appointment["patient_id"] != event.patient_id:
            raise LookupError("Appointment belongs to another patient")

        status = (
            "SCHEDULED" if event.event_type == "APPOINTMENT_SCHEDULED" else "CANCELLED"
        )
        # shortcut: arrival order wins; add version checks and locking in Phase 3.
        if appointment or event.event_type == "APPOINTMENT_CANCELLED":
            result = connection.execute(
                """
                UPDATE appointments
                SET status = %s, last_event_id = %s, updated_at = CURRENT_TIMESTAMP
                WHERE appointment_id = %s AND patient_id = %s
                """,
                (status, event.event_id, event.appointment_id, event.patient_id),
            )
            if result.rowcount == 0:
                raise LookupError("Appointment not found for the supplied patient")
        else:
            connection.execute(
                """
                INSERT INTO appointments (
                    appointment_id, patient_id, status, last_event_id
                ) VALUES (%s, %s, %s, %s)
                """,
                (event.appointment_id, event.patient_id, status, event.event_id),
            )

    return stored
