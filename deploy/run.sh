#!/bin/bash
# Rust-Rules-Engine infra runner (S0). Brings up Redpanda + ClickHouse + Postgres,
# auto-incrementing host ports past anything already bound, and waits on container
# health. No app service yet (added in S10); no /api/health endpoint until S8.

COMPOSE_FILE="$(cd "$(dirname "$0")" && pwd)/docker-compose.yml"

# ---- port helpers (ported from the Java run.sh; self-contained) -------------
is_port_open() { nc -z localhost "$1" > /dev/null 2>&1; }

ASSIGNED_PORTS=""
port_taken() {
    if is_port_open "$1"; then return 0; fi
    case " $ASSIGNED_PORTS " in
        *" $1 "*) return 0 ;;
    esac
    return 1
}

FREE_PORT=""
find_free_port() {
    local port=$1
    local label=$2
    while port_taken "$port"; do
        echo "⚠️  ${label} port $port is in use, trying $((port + 1))..."
        port=$((port + 1))
    done
    ASSIGNED_PORTS="$ASSIGNED_PORTS $port"
    FREE_PORT="$port"
}

# ---- args -------------------------------------------------------------------
FOLLOW_LOGS=false
DOWN_ONLY=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -l|--logs) FOLLOW_LOGS=true ;;
        -d|--down) DOWN_ONLY=true ;;
        -h|--help)
            echo "Usage: ./deploy/run.sh [OPTIONS]"
            echo "  -l, --logs   Follow logs after starting"
            echo "  -d, --down   Tear down the stack and exit"
            echo "  -h, --help   Show this help"
            echo ""
            echo "Port overrides (auto-incremented if busy):"
            echo "  REDPANDA_PORT (19092), REDPANDA_ADMIN_PORT (9644),"
            echo "  CLICKHOUSE_HTTP_PORT (8123), CLICKHOUSE_TCP_PORT (9000),"
            echo "  POSTGRES_PORT (5432)"
            exit 0
            ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# ---- preconditions ----------------------------------------------------------
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Start Docker and retry."
    exit 1
fi
if docker compose version > /dev/null 2>&1; then
    DC="docker compose"
else
    DC="docker-compose"
fi

# ---- teardown first ---------------------------------------------------------
# Tear down before probing ports so our own stale containers aren't misdetected
# as external services holding their ports.
echo "🛑 Stopping existing services..."
$DC -f "$COMPOSE_FILE" down --remove-orphans

if [ "$DOWN_ONLY" = true ]; then
    echo "✅ Stack torn down."
    exit 0
fi

# ---- resolve ports ----------------------------------------------------------
find_free_port "${REDPANDA_PORT:-19092}" "Redpanda";        export REDPANDA_PORT="$FREE_PORT"
find_free_port "${REDPANDA_ADMIN_PORT:-9644}" "Redpanda admin"; export REDPANDA_ADMIN_PORT="$FREE_PORT"
find_free_port "${CLICKHOUSE_HTTP_PORT:-8123}" "ClickHouse HTTP"; export CLICKHOUSE_HTTP_PORT="$FREE_PORT"
find_free_port "${CLICKHOUSE_TCP_PORT:-9000}" "ClickHouse native"; export CLICKHOUSE_TCP_PORT="$FREE_PORT"
find_free_port "${POSTGRES_PORT:-5432}" "Postgres";          export POSTGRES_PORT="$FREE_PORT"

echo "----------------------------------------------------------"
echo "🚀 Starting Rust-Rules-Engine infra"
echo "📦 Kafka (Redpanda):  localhost:$REDPANDA_PORT"
echo "🟡 ClickHouse HTTP:   localhost:$CLICKHOUSE_HTTP_PORT  (native $CLICKHOUSE_TCP_PORT)"
echo "🐘 Postgres:          postgres://rules:rules@localhost:$POSTGRES_PORT/ruleaudit"
echo "----------------------------------------------------------"

$DC -f "$COMPOSE_FILE" up -d

# ---- health wait ------------------------------------------------------------
echo "⏳ Waiting for services to become healthy..."
SERVICES="rre-redpanda rre-clickhouse rre-postgres"
MAX_ATTEMPTS=60
ATTEMPT=1
while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    ALL_HEALTHY=true
    for c in $SERVICES; do
        STATUS=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$c" 2>/dev/null || echo "missing")
        if [ "$STATUS" != "healthy" ]; then
            ALL_HEALTHY=false
        fi
    done
    if [ "$ALL_HEALTHY" = true ]; then
        echo "✅ All services healthy."
        break
    fi
    echo "   (Attempt $ATTEMPT/$MAX_ATTEMPTS) waiting..."
    sleep 2
    ATTEMPT=$((ATTEMPT + 1))
done

if [ $ATTEMPT -gt $MAX_ATTEMPTS ]; then
    echo "⚠️  Services took too long. Check: $DC -f $COMPOSE_FILE logs"
fi

echo "----------------------------------------------------------"
echo "✅ Infra is running. Teardown: ./deploy/run.sh --down"
echo "----------------------------------------------------------"

if [ "$FOLLOW_LOGS" = true ]; then
    $DC -f "$COMPOSE_FILE" logs -f
fi
