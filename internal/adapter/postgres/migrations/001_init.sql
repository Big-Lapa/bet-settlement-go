CREATE TABLE IF NOT EXISTS bets (
    id          TEXT        PRIMARY KEY,
    player_id   TEXT        NOT NULL,
    match_id    TEXT        NOT NULL,
    selection   TEXT        NOT NULL CHECK (selection IN ('HOME', 'DRAW', 'AWAY')),
    stake       BIGINT      NOT NULL CHECK (stake > 0),
    odds        BIGINT      NOT NULL CHECK (odds > 1000),
    status      TEXT        NOT NULL CHECK (status IN ('PENDING', 'WON', 'LOST', 'VOID')),
    payout      BIGINT      NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL,
    settled_at  TIMESTAMPTZ
);

-- Partial index: settlement only ever looks for pending bets of one match.
CREATE INDEX IF NOT EXISTS bets_pending_by_match_idx ON bets (match_id) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS bets_player_idx ON bets (player_id);

-- Idempotency log: one row per processed match result.
CREATE TABLE IF NOT EXISTS processed_results (
    match_id     TEXT        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox.
CREATE TABLE IF NOT EXISTS outbox (
    id         BIGSERIAL   PRIMARY KEY,
    topic      TEXT        NOT NULL,
    key        TEXT        NOT NULL,
    payload    JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_unsent_idx ON outbox (id) WHERE sent_at IS NULL;
