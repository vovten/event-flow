-- Event Outbox Table Schema
-- Copy this file to your resources/db/ directory or execute manually

-- PostgreSQL: use ENUM for status
-- MySQL/H2/Oracle: use SMALLINT (0=PENDING, 1=PUBLISHED, 2=FAILED)

-- PostgreSQL
CREATE TYPE event_status AS ENUM ('PENDING', 'PUBLISHED', 'FAILED');

CREATE TABLE public.event_outbox (
    id          UUID PRIMARY KEY,
    process_id  UUID,
    event       JSONB NOT NULL,
    status      event_status NOT NULL DEFAULT 'PENDING',
    retry       BOOLEAN NOT NULL DEFAULT FALSE,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMP NOT NULL DEFAULT NOW(),
    error_message TEXT
);

CREATE INDEX idx_event_outbox_status ON public.event_outbox (status);
CREATE INDEX idx_event_outbox_retry ON public.event_outbox (retry, status);
CREATE INDEX idx_event_outbox_failed ON public.event_outbox (status, retry_count, modified_at);