create TABLE jobs(
    job_id uuid not null PRIMARY KEY,
    status varchar(20) not null default 'pending',
    payload jsonb,
    lease TIMESTAMPTZ,
    attempt_count int default 0,
    created_at TIMESTAMP DEFAULT NOW(),
    idempotency_key VARCHAR(255) UNIQUE NOT NULL
)