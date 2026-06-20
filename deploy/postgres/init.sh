#!/bin/bash
set -e

# Create replication user and allow replication connections.
# Runs as part of postgres initdb (docker-entrypoint-initdb.d).
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator';
EOSQL

echo "host replication replicator all md5" >> "$PGDATA/pg_hba.conf"
echo "Replication user created, pg_hba.conf updated"
