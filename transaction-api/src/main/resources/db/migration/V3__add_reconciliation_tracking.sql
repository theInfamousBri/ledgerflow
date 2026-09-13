ALTER TABLE transactions
    ADD COLUMN reconciliation_requested_at TIMESTAMPTZ,
    ADD COLUMN reconciliation_attempts INTEGER NOT NULL DEFAULT 0;

CREATE INDEX idx_transactions_reconciliation_candidates
    ON transactions(updated_at, reconciliation_requested_at)
    WHERE status = 'PROCESSING';
