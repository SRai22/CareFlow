from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from app.idempotency import request_hash
from app.schemas import EventCreate, EventRead


def _insert_event(
    event: EventCreate,
    connection: psycopg.Connection[dict[str, Any]],
    notification_type: str | None = None,
) -> dict[str, Any]:
    received_at = connection.execute("SELECT CURRENT_TIMESTAMP AS received_at").fetchone()[
        "received_at"
    ]
    outcome = EventRead(**event.model_dump(), received_at=received_at).model_dump(mode="json")
    stored = connection.execute(
        """
        INSERT INTO events (
            event_id, patient_id, appointment_id,
            event_type, event_version, occurred_at, received_at,
            request_hash, response_status, response_body, notification_type
        ) VALUES (
            %(event_id)s, %(patient_id)s, %(appointment_id)s,
            %(event_type)s, %(event_version)s, %(occurred_at)s, %(received_at)s,
            %(request_hash)s, 201, %(response_body)s, %(notification_type)s
        ) RETURNING *
        """,
        {
            **event.model_dump(),
            "received_at": received_at,
            "request_hash": request_hash(event),
            "response_body": Jsonb(outcome),
            "notification_type": notification_type,
        },
    ).fetchone()
    connection.execute(
        """
        INSERT INTO outbox (event_id, appointment_id, payload)
        VALUES (%s, %s, %s)
        """,
        (event.event_id, event.appointment_id, Jsonb(event.model_dump(mode="json"))),
    )
    return stored


def schedule_appointment(
    event: EventCreate, connection: psycopg.Connection[dict[str, Any]]
) -> dict[str, Any]:
    with connection.transaction():
        stored = _insert_event(event, connection, notification_type="APPOINTMENT_SCHEDULED")
        connection.execute(
            """
            INSERT INTO appointments (
                appointment_id, patient_id, status, last_event_id
            ) VALUES (%(appointment_id)s, %(patient_id)s, 'SCHEDULED', %(event_id)s)
            """,
            event.model_dump(),
        )

    return stored


def cancel_appointment(
    event: EventCreate, connection: psycopg.Connection[dict[str, Any]]
) -> dict[str, Any]:
    with connection.transaction():
        stored = _insert_event(event, connection)
        # Serialize this transition so concurrent cancellations cannot both warrant a notice.
        previous = connection.execute(
            """
            SELECT status FROM appointments
            WHERE appointment_id = %(appointment_id)s AND patient_id = %(patient_id)s
            FOR UPDATE
            """,
            event.model_dump(),
        ).fetchone()
        if previous is None:
            raise LookupError("Appointment not found for the supplied patient")
        result = connection.execute(
            """
            UPDATE appointments
            SET status = 'CANCELLED', last_event_id = %(event_id)s,
                updated_at = CURRENT_TIMESTAMP
            WHERE appointment_id = %(appointment_id)s
              AND patient_id = %(patient_id)s
            """,
            event.model_dump(),
        )
        if result.rowcount == 0:
            raise LookupError("Appointment not found for the supplied patient")
        if previous["status"] == "SCHEDULED":
            stored = connection.execute(
                """
                UPDATE events SET notification_type = 'APPOINTMENT_CANCELLED'
                WHERE event_id = %s RETURNING *
                """,
                (event.event_id,),
            ).fetchone()

    return stored
