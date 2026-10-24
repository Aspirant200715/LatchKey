-- inventory_db: consumer inbox, stock, effect ledger, dead letters.

CREATE TABLE inventory (
  sku   text    PRIMARY KEY,
  stock integer NOT NULL CHECK (stock >= 0)
);

CREATE TABLE consumer_inbox (
  consumer_name text        NOT NULL,
  event_id      uuid        NOT NULL,
  payload_hash  bytea       NOT NULL,
  processed_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);

-- The effect ledger: one row per business effect. The verifier counts these.
CREATE TABLE inventory_reservations (
  reservation_id uuid        PRIMARY KEY,
  event_id       uuid        NOT NULL UNIQUE,  -- backstop: a second effect for one event is impossible
  order_id       uuid        NOT NULL,
  sku            text        NOT NULL,
  quantity       integer     NOT NULL,
  outcome        text        NOT NULL CHECK (outcome IN ('RESERVED', 'REJECTED_OUT_OF_STOCK')),
  created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE dead_letters (
  id            bigserial   PRIMARY KEY,
  consumer_name text        NOT NULL,
  event_id      uuid,
  topic         text        NOT NULL,
  partition     integer     NOT NULL,
  "offset"      bigint      NOT NULL,
  reason        text        NOT NULL,
  raw_message   bytea       NOT NULL,
  failed_at     timestamptz NOT NULL DEFAULT now(),
  replayed_at   timestamptz
);
