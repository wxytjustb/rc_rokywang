CREATE TABLE IF NOT EXISTS notification_event (
    id                  UUID PRIMARY KEY,

    source_system       VARCHAR(64) NOT NULL,
    source_request_id   VARCHAR(128) NOT NULL,

    provider_code       VARCHAR(64) NOT NULL,
    provider_action     VARCHAR(64) NOT NULL,

    payload             JSONB NOT NULL,

    status              VARCHAR(16) NOT NULL DEFAULT 'PENDING'
                        CHECK (status IN (
                            'PENDING',
                            'PROCESSING',
                            'SUCCEEDED',
                            'FAILED',
                            'UNKNOWN'
                        )),

    attempt_count       SMALLINT NOT NULL DEFAULT 0,
    next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    enqueued_at         TIMESTAMPTZ,

    lease_token         UUID,
    lease_until         TIMESTAMPTZ,

    last_result         JSONB,
    provider_response   JSONB,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (source_system, source_request_id)
);

CREATE INDEX IF NOT EXISTS ix_notification_event_pending
ON notification_event (
    next_attempt_at,
    enqueued_at
)
WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS ix_notification_event_expired
ON notification_event (lease_until)
WHERE status = 'PROCESSING';

CREATE INDEX IF NOT EXISTS ix_notification_event_provider_action
ON notification_event (
    provider_code,
    provider_action,
    status,
    updated_at
);
