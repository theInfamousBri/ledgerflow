ALTER TABLE outbox_events
    ADD COLUMN trace_parent VARCHAR(512),
    ADD COLUMN trace_state TEXT;
