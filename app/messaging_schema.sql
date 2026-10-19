CREATE TABLE outbox (
    event_id VARCHAR(128) PRIMARY KEY REFERENCES events(event_id),
    appointment_id VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_pending ON outbox(created_at, event_id)
WHERE published_at IS NULL;

CREATE TABLE analytics_events (
    event_id VARCHAR(128) PRIMARY KEY REFERENCES events(event_id),
    appointment_id VARCHAR(128) NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
