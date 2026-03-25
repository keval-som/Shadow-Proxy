-- =============================================================================
-- Shadow Proxy — Database Initialisation
-- Runs on first boot of postgres-primary via /docker-entrypoint-initdb.d
-- =============================================================================

-- Users table (used to demonstrate read-write splitting)
CREATE TABLE IF NOT EXISTS users (
    id          BIGSERIAL    PRIMARY KEY,
    username    VARCHAR(100) NOT NULL UNIQUE,
    email       VARCHAR(255) NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Orders table (join target for replica READ queries)
CREATE TABLE IF NOT EXISTS orders (
    id          BIGSERIAL       PRIMARY KEY,
    user_id     BIGINT          NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    product     VARCHAR(255)    NOT NULL,
    quantity    INT             NOT NULL DEFAULT 1 CHECK (quantity > 0),
    total       NUMERIC(12, 2)  NOT NULL CHECK (total >= 0),
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Indexes to make replica SELECTs realistic
CREATE INDEX IF NOT EXISTS idx_orders_user_id   ON orders (user_id);
CREATE INDEX IF NOT EXISTS idx_orders_created   ON orders (created_at DESC);

-- Seed data for local testing
INSERT INTO users (username, email) VALUES
    ('alice',   'alice@example.com'),
    ('bob',     'bob@example.com'),
    ('charlie', 'charlie@example.com')
ON CONFLICT (username) DO NOTHING;

INSERT INTO orders (user_id, product, quantity, total)
SELECT u.id, 'Widget Alpha', 2, 49.98
FROM   users u WHERE u.username = 'alice'
ON CONFLICT DO NOTHING;

INSERT INTO orders (user_id, product, quantity, total)
SELECT u.id, 'Gadget Beta', 1, 199.00
FROM   users u WHERE u.username = 'bob'
ON CONFLICT DO NOTHING;
