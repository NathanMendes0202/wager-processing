DROP INDEX IF EXISTS uq_outbox_event_per_cause;

CREATE UNIQUE INDEX uq_outbox_aggregate_version_type
    ON outbox_events(aggregate_id, version, event_type);
