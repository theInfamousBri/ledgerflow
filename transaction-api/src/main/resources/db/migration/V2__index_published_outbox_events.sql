CREATE INDEX idx_outbox_published_at
    ON outbox_events(published_at)
    WHERE published_at IS NOT NULL;
