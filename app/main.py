from typing import Annotated, Any

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Response
from psycopg.errors import UniqueViolation

from app.db import get_connection
from app.schemas import AppointmentRead, EventCreate, EventRead, Identifier

app = FastAPI(title="CareFlow", version="0.1.0")
Connection = Annotated[psycopg.Connection[dict[str, Any]], Depends(get_connection)]


@app.post("/events", response_model=EventRead, status_code=201)
def create_event(event: EventCreate, response: Response, connection: Connection):
    try:
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
                raise HTTPException(409, "Appointment belongs to another patient")

            status = (
                "SCHEDULED" if event.event_type == "APPOINTMENT_SCHEDULED" else "CANCELLED"
            )
            # shortcut: arrival order wins; add version checks and locking in Phase 3.
            if appointment:
                connection.execute(
                    """
                    UPDATE appointments
                    SET status = %s, last_event_id = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE appointment_id = %s
                    """,
                    (status, event.event_id, event.appointment_id),
                )
            else:
                connection.execute(
                    """
                    INSERT INTO appointments (
                        appointment_id, patient_id, status, last_event_id
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (event.appointment_id, event.patient_id, status, event.event_id),
                )
    except UniqueViolation as exc:
        if exc.diag.constraint_name == "events_pkey":
            raise HTTPException(409, "Event ID already exists") from exc
        raise

    response.headers["Location"] = f"/events/{event.event_id}"
    return stored


@app.get("/events/{id}", response_model=EventRead)
def get_event(id: Identifier, connection: Connection):
    event = connection.execute(
        "SELECT * FROM events WHERE event_id = %s", (id,)
    ).fetchone()
    if event is None:
        raise HTTPException(404, "Event not found")
    return event


@app.get("/appointments/{id}", response_model=AppointmentRead)
def get_appointment(id: Identifier, connection: Connection):
    appointment = connection.execute(
        "SELECT * FROM appointments WHERE appointment_id = %s", (id,)
    ).fetchone()
    if appointment is None:
        raise HTTPException(404, "Appointment not found")
    return appointment
