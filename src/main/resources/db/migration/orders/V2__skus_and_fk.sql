-- Reference data: SKUs the API accepts. An unknown SKU is a deterministic business rejection
-- (stored as FAILED_TERMINAL and replayed on retry). Stock levels live in inventory_db, not here.
CREATE TABLE skus (
  sku text PRIMARY KEY
);

-- Idempotency records expire and are cleaned up (or rotated when a key is reused after expiry).
-- Orders must outlive their idempotency record, so the foreign key is dropped.
-- orders.operation_id stays UNIQUE: the verifier still proves one order per operation.
ALTER TABLE orders DROP CONSTRAINT orders_operation_id_fkey;
