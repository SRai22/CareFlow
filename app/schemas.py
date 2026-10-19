from datetime import datetime, timezone
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, field_validator

Identifier = Annotated[
    str, StringConstraints(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
]


class EventCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: Identifier
    patient_id: Identifier
    appointment_id: Identifier
    event_type: Literal["APPOINTMENT_SCHEDULED", "APPOINTMENT_RESCHEDULED", "APPOINTMENT_CANCELLED"]
    event_version: Annotated[int, Field(strict=True, gt=0, le=2_147_483_647)]
    occurred_at: AwareDatetime
    # Nullable for historical events; new scheduling transactions require a future value.
    scheduled_for: AwareDatetime | None = None

    @field_validator("occurred_at", "scheduled_for")
    @classmethod
    def normalize_timestamps(cls, value: datetime | None) -> datetime | None:
        return value.astimezone(timezone.utc) if value is not None else None


class StoredEvent(EventCreate):
    schedule_generation: Annotated[int, Field(strict=True, gt=0)] | None = None


class EventRead(EventCreate):
    received_at: datetime


class AppointmentRead(BaseModel):
    appointment_id: str
    patient_id: str
    status: Literal["SCHEDULED", "CANCELLED"]
    last_event_id: str
    updated_at: datetime
    scheduled_for: datetime | None = None
    schedule_generation: int = 1
