DROP INDEX IF EXISTS idx_outbox_dead;
ALTER TABLE outbox_events
    DROP COLUMN IF EXISTS dead_reason,
    DROP COLUMN IF EXISTS dead_at;
