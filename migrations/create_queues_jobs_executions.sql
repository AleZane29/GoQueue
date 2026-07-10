CREATE TABLE queues (
    id      SERIAL PRIMARY KEY,
    name    TEXT NOT NULL UNIQUE,          -- "high", "medium", "low"
    weight  INT  NOT NULL DEFAULT 1        -- weighted polling
);

CREATE TABLE jobs (
    id                 UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id           INT         NOT NULL REFERENCES queues(id),
    name               TEXT        NOT NULL,  -- es. "send_email"
    status             TEXT        NOT NULL DEFAULT 'pending', -- pending | running | completed | failed | dead
    type               TEXT        NOT NULL,  -- handler da invocare
    payload            JSONB       NOT NULL DEFAULT '{}',
    max_time_to_execute INTERVAL   NOT NULL DEFAULT '5 minutes',
    max_attempts       INT         NOT NULL DEFAULT 3,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    scheduled_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE executions (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id       UUID        NOT NULL REFERENCES jobs(id),
    worker_id    TEXT        NOT NULL,
    attempt      INT         NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    status       TEXT        NOT NULL,  -- running | completed | failed
    error        TEXT                   -- messaggio di errore se fallito
);

-- Index for scheduler performance
CREATE INDEX idx_jobs_queue_status_scheduled
    ON jobs (queue_id, status, scheduled_at)
    WHERE status = 'pending';