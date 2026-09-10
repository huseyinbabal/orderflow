-- CDC-outbox vs event-sourcing branch.
-- Two ways to get order events onto Kafka, side by side.

-- ============================================================
-- Path A — CDC + Transactional Outbox
-- ============================================================

-- The business table. The current state of an order, nothing more.
CREATE TABLE IF NOT EXISTS orders (
    id          UUID PRIMARY KEY,
    customer_id TEXT NOT NULL,
    amount      NUMERIC(12,2) NOT NULL,
    status      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The outbox. Written in the SAME transaction as the business row — that is the
-- whole trick: one commit, both rows, no dual-write. Debezium tails this table's
-- WAL and the EventRouter SMT turns each row into a Kafka message, then the row
-- can be pruned. The app never talks to Kafka on this path.
CREATE TABLE IF NOT EXISTS outbox (
    id             UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,          -- routes the topic: outbox.event.<aggregate_type>
    aggregate_id   TEXT NOT NULL,          -- becomes the Kafka message key
    type           TEXT NOT NULL,          -- OrderPlaced, OrderCancelled, …
    payload        JSONB NOT NULL,         -- the event body, verbatim on the wire
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- Path B — Event Sourcing
-- ============================================================

-- The event store IS the database. There is no "orders" row on this path — the
-- order's state is the left fold of its events. Append-only; (aggregate_id,
-- version) is unique, so two concurrent writers can't both append version N.
CREATE TABLE IF NOT EXISTS es_event (
    seq          BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    version      INT  NOT NULL,
    type         TEXT NOT NULL,            -- OrderOpened, ItemAdded, OrderCheckedOut
    payload      JSONB NOT NULL,
    at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (aggregate_id, version)
);
