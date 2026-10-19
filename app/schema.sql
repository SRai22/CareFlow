CREATE TABLE events (
    event_id VARCHAR(128) PRIMARY KEY,
    patient_id VARCHAR(128) NOT NULL,
    appointment_id VARCHAR(128) NOT NULL,
    event_type TEXT NOT NULL CHECK (
        event_type IN ('APPOINTMENT_SCHEDULED', 'APPOINTMENT_CANCELLED')
    ),
    event_version INTEGER NOT NULL CHECK (event_version > 0),
    occurred_at TIMESTAMPTZ NOT NULL,
    scheduled_for TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    request_hash CHAR(64) NOT NULL,
    response_status SMALLINT NOT NULL CHECK (response_status BETWEEN 100 AND 599),
    response_body JSONB NOT NULL,
    notification_type TEXT CHECK (
        notification_type IN ('APPOINTMENT_SCHEDULED', 'APPOINTMENT_CANCELLED')
    )
);

CREATE INDEX idx_events_appointment_id
ON events(appointment_id);

CREATE TABLE appointments (
    appointment_id VARCHAR(128) PRIMARY KEY,
    patient_id VARCHAR(128) NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('SCHEDULED', 'CANCELLED')),
    last_event_id VARCHAR(128) NOT NULL REFERENCES events(event_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    scheduled_for TIMESTAMPTZ
);
