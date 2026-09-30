DROP INDEX IF EXISTS idx_wager_pending_reference;
ALTER TABLE wager_transactions
    DROP COLUMN IF EXISTS last_reference_error,
    DROP COLUMN IF EXISTS next_reference_attempt_at,
    DROP COLUMN IF EXISTS reference_attempts;
