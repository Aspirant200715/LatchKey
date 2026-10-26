DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'latchkey_consumer') THEN
    CREATE ROLE latchkey_consumer LOGIN;
  END IF;
END
$$;

\getenv consumer_password INVENTORY_DB_PASSWORD
ALTER ROLE latchkey_consumer PASSWORD :'consumer_password';

SELECT 'CREATE DATABASE inventory_db OWNER latchkey_consumer'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'inventory_db')
\gexec

ALTER DATABASE inventory_db OWNER TO latchkey_consumer;
REVOKE CONNECT ON DATABASE orders_db FROM PUBLIC;
REVOKE CONNECT ON DATABASE inventory_db FROM PUBLIC;
GRANT CONNECT ON DATABASE inventory_db TO latchkey_consumer;
