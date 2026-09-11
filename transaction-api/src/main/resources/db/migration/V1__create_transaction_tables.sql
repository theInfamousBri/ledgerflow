CREATE TABLE transactions (
    id UUID PRIMARY KEY,
    idempotency_key VARCHAR(100) NOT NULL UNIQUE,
    request_fingerprint VARCHAR(64) NOT NULL,
    account_id VARCHAR(100) NOT NULL,
    amount NUMERIC(19,2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL,
    type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    provider_reference VARCHAR(100),
    failure_code VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE transaction_status_history (
    id UUID PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES transactions(id),
    status VARCHAR(20) NOT NULL,
    event_id UUID NOT NULL UNIQUE,
    reason VARCHAR(200),
    occurred_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_transaction_history_transaction_time
    ON transaction_status_history(transaction_id, occurred_at);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    topic VARCHAR(200) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished
    ON outbox_events(created_at) WHERE published_at IS NULL;

