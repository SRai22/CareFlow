from datetime import datetime
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints

Identifier = Annotated[
    str, StringConstraints(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
]


class EventCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: Identifier
    patient_id: Identifier
    appointment_id: Identifier
    event_type: Literal["APPOINTMENT_SCHEDULED", "APPOINTMENT_CANCELLED"]
    event_version: Annotated[int, Field(strict=True, gt=0, le=2_147_483_647)]
    occurred_at: AwareDatetime


class EventRead(EventCreate):
    received_at: datetime


class AppointmentRead(BaseModel):
    appointment_id: str
    patient_id: str
    status: Literal["SCHEDULED", "CANCELLED"]
    last_event_id: str
    updated_at: datetime
