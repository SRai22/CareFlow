from typing import Annotated, Any

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Response
from psycopg.errors import UniqueViolation

from app.db import get_connection
from app.schemas import AppointmentRead, EventCreate, EventRead, Identifier
from app.transactions import process_event

app = FastAPI(title="CareFlow", version="0.1.0")
Connection = Annotated[psycopg.Connection[dict[str, Any]], Depends(get_connection)]


@app.post("/events", response_model=EventRead, status_code=201)
def create_event(event: EventCreate, response: Response, connection: Connection):
    try:
        stored = process_event(event, connection)
    except LookupError as exc:
        raise HTTPException(409, str(exc)) from exc
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
