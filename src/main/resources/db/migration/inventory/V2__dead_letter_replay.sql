-- Preserve the Kafka envelope so an operator can replay the original record.
ALTER TABLE dead_letters
  ADD COLUMN record_key text,
  ADD COLUMN headers jsonb;
