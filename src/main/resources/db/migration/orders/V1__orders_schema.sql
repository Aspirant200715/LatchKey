-- orders_db: idempotency records, orders, transactional outbox.
-- All timing uses the database clock (now()), never application clocks.

CREATE TABLE tenants (
  tenant_id      uuid  PRIMARY KEY,
  name           text  NOT NULL,
  api_key_sha256 bytea NOT NULL UNIQUE
);

CREATE TABLE idempotency_records (
  tenant_id        uuid        NOT NULL REFERENCES tenants,
  operation_name   text        NOT NULL,
  idempotency_key  text        NOT NULL CHECK (char_length(idempotency_key) BETWEEN 1 AND 255),
  operation_id     uuid        NOT NULL UNIQUE,
  payload_hash     bytea       NOT NULL,
  status           text        NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED_TERMINAL')),
  response_code    integer,
  response_body    jsonb,
  lease_expires_at timestamptz,
  attempt          integer     NOT NULL DEFAULT 1,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  expires_at       timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, operation_name, idempotency_key)
);
CREATE INDEX idempotency_records_expires_idx ON idempotency_records (expires_at);

CREATE TABLE orders (
  order_id     uuid        PRIMARY KEY,
  tenant_id    uuid        NOT NULL REFERENCES tenants,
  operation_id uuid        NOT NULL UNIQUE REFERENCES idempotency_records (operation_id),
  sku          text        NOT NULL,
  quantity     integer     NOT NULL CHECK (quantity > 0),
  status       text        NOT NULL,
  version      integer     NOT NULL DEFAULT 1,
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE outbox_events (
  event_id          uuid        PRIMARY KEY,   -- generated in the producing transaction, never regenerated
  tenant_id         uuid        NOT NULL,
  aggregate_type    text        NOT NULL,
  aggregate_id      uuid        NOT NULL,
  aggregate_version integer     NOT NULL,
  partition_key     text        NOT NULL,
  event_type        text        NOT NULL,
  schema_version    integer     NOT NULL DEFAULT 1,
  payload           jsonb       NOT NULL,
  headers           jsonb       NOT NULL DEFAULT '{}',  -- traceparent etc.
  created_at        timestamptz NOT NULL DEFAULT now(),
  published_at      timestamptz,
  attempts          integer     NOT NULL DEFAULT 0,
  next_attempt_at   timestamptz NOT NULL DEFAULT now(),
  locked_by         text,
  locked_until      timestamptz,
  last_error        text
);
CREATE INDEX outbox_unpublished_idx ON outbox_events (next_attempt_at) WHERE published_at IS NULL;
