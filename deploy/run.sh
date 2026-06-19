#!/bin/bash
# Rust-Rules-Engine full-stack runner (S10).
# Starts all services: Redpanda, ClickHouse, Postgres, rules-engine app, frontend, SRE agent.

COMPOSE_FILE="$(cd "$(dirname "$0")" && pwd)/docker-compose.yml"

# ---- port helpers -----------------------------------------------------------
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
        echo "  ${label} port $port is in use, trying $((port + 1))..."
        port=$((port + 1))
    done
    ASSIGNED_PORTS="$ASSIGNED_PORTS $port"
    FREE_PORT="$port"
}

# ---- args -------------------------------------------------------------------
FOLLOW_LOGS=false
DOWN_ONLY=false
BUILD_IMAGES=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -l|--logs)   FOLLOW_LOGS=true ;;
        -d|--down)   DOWN_ONLY=true ;;
        -b|--build)  BUILD_IMAGES=true ;;
        -h|--help)
            echo "Usage: ./deploy/run.sh [OPTIONS]"
            echo "  -l, --logs   Follow logs after starting"
            echo "  -d, --down   Tear down the stack and exit"
            echo "  -b, --build  Force Docker image rebuild"
            echo "  -h, --help   Show this help"
            echo ""
            echo "Port overrides (auto-incremented if busy):"
            echo "  REDPANDA_PORT (19092), REDPANDA_ADMIN_PORT (9644),"
            echo "  CLICKHOUSE_HTTP_PORT (8123), CLICKHOUSE_TCP_PORT (9000),"
            echo "  POSTGRES_PORT (5432), APP_PORT (8080), FRONTEND_PORT (3000),"
            echo "  SRE_PORT (8088)"
            exit 0
            ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# ---- preconditions ----------------------------------------------------------
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Start Docker and retry."
    exit 1
fi

# Load UNSLOTH_API_KEY from ~/.zshrc if not already set, so the SRE agent LLM works.
if [ -z "${UNSLOTH_API_KEY:-}" ] && [ -f "$HOME/.zshrc" ]; then
    UNSLOTH_API_KEY="$(source "$HOME/.zshrc" 2>/dev/null; echo "${UNSLOTH_API_KEY:-}")"
    export UNSLOTH_API_KEY
fi
if docker compose version > /dev/null 2>&1; then
    DC="docker compose"
else
    DC="docker-compose"
fi

# ---- teardown first ---------------------------------------------------------
echo "Stopping existing services..."
$DC -f "$COMPOSE_FILE" down --remove-orphans

if [ "$DOWN_ONLY" = true ]; then
    echo "Stack torn down."
    exit 0
fi

# ---- resolve ports ----------------------------------------------------------
find_free_port "${REDPANDA_PORT:-19092}"       "Redpanda";         export REDPANDA_PORT="$FREE_PORT"
find_free_port "${REDPANDA_ADMIN_PORT:-9644}"  "Redpanda admin";   export REDPANDA_ADMIN_PORT="$FREE_PORT"
find_free_port "${CLICKHOUSE_HTTP_PORT:-8123}" "ClickHouse HTTP";  export CLICKHOUSE_HTTP_PORT="$FREE_PORT"
find_free_port "${CLICKHOUSE_TCP_PORT:-9000}"  "ClickHouse native";export CLICKHOUSE_TCP_PORT="$FREE_PORT"
find_free_port "${POSTGRES_PORT:-5432}"        "Postgres";         export POSTGRES_PORT="$FREE_PORT"
find_free_port "${APP_PORT:-8080}"             "Rules-Engine app"; export APP_PORT="$FREE_PORT"
find_free_port "${FRONTEND_PORT:-3000}"        "Frontend";         export FRONTEND_PORT="$FREE_PORT"
find_free_port "${SRE_PORT:-8088}"             "SRE dashboard";    export SRE_PORT="$FREE_PORT"

echo "----------------------------------------------------------"
echo "Starting Rust-Rules-Engine full stack"
echo "  Kafka (Redpanda): localhost:$REDPANDA_PORT"
echo "  ClickHouse HTTP:  localhost:$CLICKHOUSE_HTTP_PORT"
echo "  Postgres:         localhost:$POSTGRES_PORT"
echo "  Rules Engine API: http://localhost:$APP_PORT"
echo "  Frontend UI:      http://localhost:$FRONTEND_PORT"
echo "  SRE dashboard:    http://localhost:$SRE_PORT"
echo "----------------------------------------------------------"

BUILD_FLAG=""
[ "$BUILD_IMAGES" = true ] && BUILD_FLAG="--build"

$DC -f "$COMPOSE_FILE" up -d $BUILD_FLAG

# ---- Kafka topic setup (wait for Redpanda, then create topics) --------------
echo "Waiting for Redpanda..."
until docker exec rre-redpanda rpk cluster info --brokers localhost:9092 > /dev/null 2>&1; do
    sleep 2
done

SOURCE_TOPIC="${SOURCE_TOPIC:-source-events}"
TARGET_TOPIC="${TARGET_TOPIC:-target-events}"

for topic in "$SOURCE_TOPIC" "$TARGET_TOPIC"; do
    docker exec rre-redpanda rpk topic create "$topic" \
        --brokers localhost:9092 \
        --partitions 3 \
        --replicas 1 2>/dev/null || true
done
echo "  Kafka topics ready: $SOURCE_TOPIC, $TARGET_TOPIC"

# ---- health wait ------------------------------------------------------------
echo "Waiting for all services to become healthy..."
SERVICES="rre-redpanda rre-clickhouse rre-postgres rre-app rre-sre-agent"
MAX_ATTEMPTS=90
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
        echo "All services healthy."
        break
    fi
    echo "  (Attempt $ATTEMPT/$MAX_ATTEMPTS) waiting..."
    sleep 3
    ATTEMPT=$((ATTEMPT + 1))
done

if [ $ATTEMPT -gt $MAX_ATTEMPTS ]; then
    echo "WARNING: Some services took too long. Check: $DC -f $COMPOSE_FILE ps"
fi

echo "----------------------------------------------------------"
echo "Stack is running."
echo "  Frontend UI:    http://localhost:$FRONTEND_PORT"
echo "  Rules Engine:   http://localhost:$APP_PORT/health"
echo "  SRE Agent:      http://localhost:$SRE_PORT"
echo "  Teardown:       ./deploy/run.sh --down"
echo "----------------------------------------------------------"

if [ "$FOLLOW_LOGS" = true ]; then
    $DC -f "$COMPOSE_FILE" logs -f
fi
