CREATE TABLE notifications (
    event_id VARCHAR(128) NOT NULL REFERENCES events(event_id),
    notification_type TEXT NOT NULL CHECK (
        notification_type IN ('APPOINTMENT_SCHEDULED', 'APPOINTMENT_CANCELLED')
    ),
    provider_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'SENT')),
    provider_message_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at TIMESTAMPTZ,
    PRIMARY KEY (event_id, notification_type),
    CHECK (
        (status = 'PENDING' AND provider_message_id IS NULL AND sent_at IS NULL)
        OR (status = 'SENT' AND provider_message_id IS NOT NULL AND sent_at IS NOT NULL)
    )
);

-- Independent provider storage: its acceptance commits separately from worker state.
CREATE TABLE mock_provider_acceptances (
    idempotency_key TEXT PRIMARY KEY,
    payload_hash CHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    message_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
