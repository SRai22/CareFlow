CREATE TABLE events (
    event_id VARCHAR(128) PRIMARY KEY,
    patient_id VARCHAR(128) NOT NULL,
    appointment_id VARCHAR(128) NOT NULL,
    event_type TEXT NOT NULL CHECK (
        event_type IN ('APPOINTMENT_SCHEDULED', 'APPOINTMENT_CANCELLED')
    ),
    event_version INTEGER NOT NULL CHECK (event_version > 0),
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE appointments (
    appointment_id VARCHAR(128) PRIMARY KEY,
    patient_id VARCHAR(128) NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('SCHEDULED', 'CANCELLED')),
    last_event_id VARCHAR(128) NOT NULL REFERENCES events(event_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
