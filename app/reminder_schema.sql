ALTER TABLE appointments ADD COLUMN schedule_generation INTEGER NOT NULL DEFAULT 1 CHECK (schedule_generation > 0);
ALTER TABLE events ADD COLUMN schedule_generation INTEGER CHECK (schedule_generation > 0);
ALTER TABLE events DROP CONSTRAINT events_event_type_check;
ALTER TABLE events ADD CONSTRAINT events_event_type_check CHECK (
    event_type IN ('APPOINTMENT_SCHEDULED', 'APPOINTMENT_RESCHEDULED', 'APPOINTMENT_CANCELLED')
);
ALTER TABLE notifications DROP CONSTRAINT notifications_notification_type_check;
ALTER TABLE notifications ADD CONSTRAINT notifications_notification_type_check CHECK (
    notification_type IN ('APPOINTMENT_SCHEDULED', 'APPOINTMENT_CANCELLED', 'APPOINTMENT_REMINDER')
);
ALTER TABLE notifications DROP CONSTRAINT notifications_status_check;
ALTER TABLE notifications DROP CONSTRAINT notifications_check;
ALTER TABLE notifications ADD CONSTRAINT notifications_status_check CHECK (status IN ('PENDING', 'SENT', 'SUPPRESSED', 'UNKNOWN'));
ALTER TABLE notifications ADD CONSTRAINT notifications_check CHECK (
    (status <> 'SENT' AND provider_message_id IS NULL AND sent_at IS NULL)
    OR (status = 'SENT' AND provider_message_id IS NOT NULL AND sent_at IS NOT NULL)
);
CREATE TABLE notification_deliveries (
    appointment_id VARCHAR(128) NOT NULL REFERENCES appointments(appointment_id),
    schedule_generation INTEGER NOT NULL CHECK (schedule_generation > 0),
    notification_type TEXT NOT NULL DEFAULT 'APPOINTMENT_REMINDER' CHECK (notification_type = 'APPOINTMENT_REMINDER'),
    event_id VARCHAR(128) NOT NULL,
    provider_key TEXT NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'DISPATCHING', 'SENT', 'SUPPRESSED', 'UNCERTAIN')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_attempt_at TIMESTAMPTZ,
    last_error TEXT,
    claimed_at TIMESTAMPTZ,
    invalidated_at TIMESTAMPTZ,
    invalidation_reason TEXT,
    provider_message_id UUID,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (appointment_id, schedule_generation, notification_type),
    UNIQUE (event_id, notification_type),
    FOREIGN KEY (event_id, notification_type) REFERENCES notifications(event_id, notification_type),
    CHECK ((status = 'SENT') = (provider_message_id IS NOT NULL))
);
CREATE TABLE reminder_handoffs (
    event_id VARCHAR(128) PRIMARY KEY REFERENCES events(event_id),
    outcome JSONB NOT NULL,
    acknowledged_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
