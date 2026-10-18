from typing import Annotated, Any

import psycopg
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from psycopg.errors import UniqueViolation

from app.db import get_connection
from app.idempotency import request_hash
from app.schemas import AppointmentRead, EventCreate, EventRead, Identifier
from app.transactions import cancel_appointment, schedule_appointment

app = FastAPI(title="CareFlow", version="0.1.0")
Connection = Annotated[psycopg.Connection[dict[str, Any]], Depends(get_connection)]


@app.post(
    "/events",
    response_model=EventRead,
    status_code=201,
    responses={
        404: {"description": "Appointment not found for the supplied patient"},
        409: {"description": "Event ID reused with different content or appointment already exists"},
        500: {"description": "Internal server error"},
    },
)
def create_event(event: EventCreate, connection: Connection):
    try:
        if event.event_type == "APPOINTMENT_SCHEDULED":
            stored = schedule_appointment(event, connection)
        else:
            stored = cancel_appointment(event, connection)
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except UniqueViolation as exc:
        if exc.diag.constraint_name == "events_pkey":
            # The failed transaction has rolled back before reading the winning outcome.
            stored = connection.execute(
                "SELECT * FROM events WHERE event_id = %s", (event.event_id,)
            ).fetchone()
            if stored is None:
                raise
            if stored["request_hash"] != request_hash(event):
                raise HTTPException(409, "Event ID already exists with a different payload") from exc
        elif exc.diag.constraint_name == "appointments_pkey":
            raise HTTPException(409, "Appointment already exists") from exc
        else:
            raise

    return JSONResponse(
        status_code=stored["response_status"],
        content=stored["response_body"],
        headers={"Location": f"/events/{event.event_id}"},
    )


@app.get("/events/{id}", response_model=EventRead)
def get_event(id: Identifier, connection: Connection):
    event = connection.execute(
        """
        SELECT event_id, patient_id, appointment_id, event_type,
               event_version, occurred_at, received_at
        FROM events WHERE event_id = %s
        """,
        (id,),
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
