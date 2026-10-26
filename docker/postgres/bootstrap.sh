#!/bin/sh
set -eu

until pg_isready --host postgres --port 5432 --username latchkey --dbname orders_db; do
  sleep 1
done

export PGPASSWORD="$POSTGRES_PASSWORD"
psql --host postgres --username latchkey --dbname orders_db \
  --file=/bootstrap.sql
