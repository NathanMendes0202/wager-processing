-- Reference retry metadata for asynchronous recovery of REFUND/ROLLBACK transactions.
ALTER TABLE wager_transactions
    ADD COLUMN reference_attempts INT NOT NULL DEFAULT 0 CHECK (reference_attempts >= 0),
    ADD COLUMN next_reference_attempt_at TIMESTAMPTZ,
    ADD COLUMN last_reference_error TEXT;

CREATE INDEX idx_wager_pending_reference
    ON wager_transactions(next_reference_attempt_at, created_at)
    WHERE status = 'PENDING_REFERENCE';
