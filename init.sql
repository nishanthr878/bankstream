CREATE TABLE accounts (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_number VARCHAR(20) UNIQUE NOT NULL,
    owner_name    VARCHAR(255) NOT NULL,
    account_type  VARCHAR(20) NOT NULL CHECK (account_type IN ('SAVINGS', 'CURRENT')),
    tier          VARCHAR(20) NOT NULL DEFAULT 'STANDARD' CHECK (tier IN ('STANDARD', 'PREMIUM', 'ELITE')),
    balance       NUMERIC(15,2) NOT NULL DEFAULT 0.00,
    status        VARCHAR(20) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUSPENDED', 'CLOSED')),
    created_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE transactions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      UUID NOT NULL REFERENCES accounts(id),
    amount          NUMERIC(15,2) NOT NULL,
    currency        VARCHAR(3) NOT NULL DEFAULT 'INR',
    type            VARCHAR(20) NOT NULL CHECK (type IN ('DEBIT', 'CREDIT')),
    status          VARCHAR(20) NOT NULL DEFAULT 'INITIATED' CHECK (status IN ('INITIATED', 'COMPLETED', 'FAILED')),
    description     TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic           VARCHAR(255) NOT NULL,
    partition_key   VARCHAR(255),
    payload         JSONB NOT NULL,
    published       BOOLEAN NOT NULL DEFAULT false,
    published_at    TIMESTAMP,
    retry_count     INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_unpublished ON outbox(created_at)
    WHERE published = false;

CREATE TABLE processed_events (
    event_id      UUID PRIMARY KEY,
    consumer_group VARCHAR(255) NOT NULL,
    processed_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE fraud_alerts (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id    UUID NOT NULL REFERENCES accounts(id),
    alert_type    VARCHAR(50) NOT NULL,
    details       JSONB NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO accounts (id, account_number, owner_name, account_type, tier, balance) VALUES
    ('a0000001-0000-0000-0000-000000000001', 'ACC001', 'Nishanth Kumar', 'SAVINGS', 'PREMIUM', 50000.00),
    ('a0000001-0000-0000-0000-000000000002', 'ACC002', 'Ravi Shankar',   'CURRENT', 'STANDARD', 10000.00),
    ('a0000001-0000-0000-0000-000000000003', 'ACC003', 'Priya Mehta',    'SAVINGS', 'ELITE',   200000.00);