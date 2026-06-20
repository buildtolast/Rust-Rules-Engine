#!/bin/bash
set -e

DATA_DIR=/var/lib/postgresql/data

mkdir -p "$DATA_DIR"
chown postgres:postgres "$DATA_DIR"
chmod 0700 "$DATA_DIR"

if [ ! -f "$DATA_DIR/PG_VERSION" ]; then
    echo "[replica] Data directory empty — running pg_basebackup from primary..."
    until gosu postgres PGPASSWORD=replicator pg_basebackup \
        -h postgres \
        -U replicator \
        -D "$DATA_DIR" \
        -Fp -Xs -P -R \
        --checkpoint=fast; do
        echo "[replica] Primary not ready, retrying in 5s..."
        sleep 5
    done
    echo "[replica] pg_basebackup complete, starting in hot-standby mode"
else
    echo "[replica] Data directory exists, starting in hot-standby mode"
fi

exec gosu postgres postgres -c hot_standby=on
