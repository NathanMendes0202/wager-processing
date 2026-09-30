ALTER TABLE outbox_events
    ADD COLUMN dead_at TIMESTAMPTZ,
    ADD COLUMN dead_reason TEXT;

CREATE INDEX idx_outbox_dead
    ON outbox_events(dead_at)
    WHERE dead_at IS NOT NULL;
