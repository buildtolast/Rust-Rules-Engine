#!/bin/bash
# Rust-Rules-Engine End-to-End Runner
# Starts the full stack: Redpanda, ClickHouse, Postgres, rules-engine, frontend, SRE agent.

set -e

DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$DEPLOY_DIR/docker-compose.yml"
COMPOSE_OBS="$DEPLOY_DIR/docker-compose.observability.yml"

# ── Default port exports ──────────────────────────────────────────────────────
export REDPANDA_PORT=${REDPANDA_PORT:-19092}
export REDPANDA_ADMIN_PORT=${REDPANDA_ADMIN_PORT:-9644}
export CLICKHOUSE_HTTP_PORT=${CLICKHOUSE_HTTP_PORT:-8123}
export CLICKHOUSE_TCP_PORT=${CLICKHOUSE_TCP_PORT:-9000}
export POSTGRES_PORT=${POSTGRES_PORT:-5432}
export APP_PORT=${APP_PORT:-8080}
export FRONTEND_PORT=${FRONTEND_PORT:-3000}
export SRE_PORT=${SRE_PORT:-8088}
export SIGNOZ_PORT=${SIGNOZ_PORT:-3301}

# ── Connection overrides (skip the matching container when set) ───────────────
KAFKA_BROKERS_OVERRIDE=${KAFKA_BROKERS:-}
DATABASE_URL_OVERRIDE=${DATABASE_URL:-}
CLICKHOUSE_URL_OVERRIDE=${CLICKHOUSE_URL:-}

# ── Port helpers ──────────────────────────────────────────────────────────────
is_port_open() { nc -z localhost "$1" > /dev/null 2>&1; }

ASSIGNED_PORTS=""
port_taken() {
    if is_port_open "$1"; then return 0; fi
    case " $ASSIGNED_PORTS " in *" $1 "*) return 0 ;; esac
    return 1
}

FREE_PORT=""
find_free_port() {
    local port=$1 label=$2
    while port_taken "$port"; do
        echo "⚠️  ${label} port $port is in use, trying $((port + 1))..."
        port=$((port + 1))
    done
    ASSIGNED_PORTS="$ASSIGNED_PORTS $port"
    FREE_PORT="$port"
}

# ── Argument parsing ──────────────────────────────────────────────────────────
# Default: cache-aware build (cargo layer cache is the Rust build cache;
#          use --rebuild for a full --no-cache clean build).
FORCE_REBUILD=false
FAST_MODE=false        # --fast: skip build, just re-up containers
FOLLOW_LOGS=false
DOWN_ONLY=false
OBS_MODE=false         # --obs:  also start SigNoz observability overlay

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --rebuild|-b)    FORCE_REBUILD=true ;;
        --fast|-f)       FAST_MODE=true ;;
        --logs|-l)       FOLLOW_LOGS=true ;;
        --down|-d)       DOWN_ONLY=true ;;
        --obs|-o)        OBS_MODE=true ;;
        --help|-h)
            echo "Usage: ./deploy/run.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --rebuild, -b   Clean rebuild of all Docker images (--no-cache; slow for Rust)"
            echo "  --fast,    -f   Skip build, restart containers only"
            echo "  --logs,    -l   Follow logs after starting the stack"
            echo "  --down,    -d   Tear down the stack and exit"
            echo "  --obs,     -o   Also start SigNoz observability stack (OTEL collector + SigNoz UI)"
            echo "  --help,    -h   Show this help message"
            echo ""
            echo "Port overrides (auto-incremented if already in use):"
            echo "  REDPANDA_PORT        Kafka/Redpanda external port  (default: 19092)"
            echo "  REDPANDA_ADMIN_PORT  Redpanda admin port           (default: 9644)"
            echo "  CLICKHOUSE_HTTP_PORT ClickHouse HTTP port          (default: 8123)"
            echo "  CLICKHOUSE_TCP_PORT  ClickHouse native port        (default: 9000)"
            echo "  POSTGRES_PORT        PostgreSQL port               (default: 5432)"
            echo "  APP_PORT             Rules Engine API port         (default: 8080)"
            echo "  FRONTEND_PORT        Frontend UI port              (default: 3000)"
            echo "  SRE_PORT             SRE agent port                (default: 8088)"
            echo "  SIGNOZ_PORT          SigNoz UI port                (default: 3301)  [--obs only]"
            echo ""
            echo "External service overrides (skips starting the matching container):"
            echo "  KAFKA_BROKERS   External Kafka broker  (e.g., localhost:9092)"
            echo "  DATABASE_URL    External Postgres URL  (e.g., postgres://rules:rules@host/ruleaudit)"
            echo "  CLICKHOUSE_URL  External ClickHouse URL (e.g., http://localhost:8123)"
            echo ""
            echo "Example:"
            echo "  ./deploy/run.sh                        # cached build (default)"
            echo "  ./deploy/run.sh --rebuild --logs       # clean build, then follow logs"
            echo "  ./deploy/run.sh --fast                 # restart without rebuilding"
            echo "  ./deploy/run.sh --obs                  # include SigNoz tracing UI"
            echo "  FRONTEND_PORT=9000 ./deploy/run.sh"
            exit 0
            ;;
        *) echo "❌ Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# ── Preconditions ─────────────────────────────────────────────────────────────
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Start Docker Desktop and retry."
    exit 1
fi

if docker compose version > /dev/null 2>&1; then
    DC="docker compose"
else
    DC="docker-compose"
fi

# dc_run <args...> — wraps $DC with the correct compose files.
dc_run() {
    if [ "$OBS_MODE" = true ]; then
        $DC -f "$COMPOSE_FILE" -f "$COMPOSE_OBS" "$@"
    else
        $DC -f "$COMPOSE_FILE" "$@"
    fi
}

# Load UNSLOTH_API_KEY from ~/.zshrc if not already in the environment.
if [ -z "${UNSLOTH_API_KEY:-}" ] && [ -f "$HOME/.zshrc" ]; then
    UNSLOTH_API_KEY="$(source "$HOME/.zshrc" 2>/dev/null; echo "${UNSLOTH_API_KEY:-}")"
    export UNSLOTH_API_KEY
fi

# ── Teardown ──────────────────────────────────────────────────────────────────
echo "🛑 Stopping existing services..."
dc_run down --remove-orphans

if [ "$DOWN_ONLY" = true ]; then
    echo "✅ Stack torn down."
    exit 0
fi

# ── Resolve ports ─────────────────────────────────────────────────────────────
find_free_port "$REDPANDA_PORT"       "Redpanda";          export REDPANDA_PORT="$FREE_PORT"
find_free_port "$REDPANDA_ADMIN_PORT" "Redpanda admin";    export REDPANDA_ADMIN_PORT="$FREE_PORT"
find_free_port "$CLICKHOUSE_HTTP_PORT" "ClickHouse HTTP";  export CLICKHOUSE_HTTP_PORT="$FREE_PORT"
find_free_port "$CLICKHOUSE_TCP_PORT" "ClickHouse native"; export CLICKHOUSE_TCP_PORT="$FREE_PORT"
find_free_port "$POSTGRES_PORT"       "Postgres";          export POSTGRES_PORT="$FREE_PORT"
find_free_port "$APP_PORT"            "Rules Engine";      export APP_PORT="$FREE_PORT"
find_free_port "$FRONTEND_PORT"       "Frontend";          export FRONTEND_PORT="$FREE_PORT"
find_free_port "$SRE_PORT"            "SRE agent";         export SRE_PORT="$FREE_PORT"
if [ "$OBS_MODE" = true ]; then
    find_free_port "$SIGNOZ_PORT" "SigNoz UI"; export SIGNOZ_PORT="$FREE_PORT"
fi

# ── Log external overrides ────────────────────────────────────────────────────
[ -n "$KAFKA_BROKERS_OVERRIDE" ]    && echo "🌐 Using external Kafka:      $KAFKA_BROKERS_OVERRIDE"
[ -n "$DATABASE_URL_OVERRIDE" ]     && echo "🌐 Using external Postgres:   $DATABASE_URL_OVERRIDE"
[ -n "$CLICKHOUSE_URL_OVERRIDE" ]   && echo "🌐 Using external ClickHouse: $CLICKHOUSE_URL_OVERRIDE"

echo "----------------------------------------------------------"
echo "🚀 Starting Rust-Rules-Engine Stack"
echo "----------------------------------------------------------"
echo "📦 Kafka (Redpanda):  localhost:$REDPANDA_PORT"
echo "🗄️  ClickHouse HTTP:   localhost:$CLICKHOUSE_HTTP_PORT"
echo "🐘 Postgres:          localhost:$POSTGRES_PORT"
echo "⚙️  Rules Engine API:  http://localhost:$APP_PORT"
echo "🖥️  Frontend UI:       http://localhost:$FRONTEND_PORT"
echo "🛡️  SRE Agent:         http://localhost:$SRE_PORT"
if [ "$OBS_MODE" = true ]; then
echo "🔭 SigNoz UI:          http://localhost:$SIGNOZ_PORT"
echo "   OTEL gRPC:          localhost:4317"
fi
echo "----------------------------------------------------------"

# ── Build & start ─────────────────────────────────────────────────────────────
if [ "$FORCE_REBUILD" = true ]; then
    echo "🔄 Forcing a clean rebuild (--no-cache)..."
    dc_run build --no-cache
    dc_run up -d --remove-orphans
elif [ "$FAST_MODE" = true ]; then
    echo "⚡ Fast mode — skipping build, restarting containers..."
    dc_run up -d --remove-orphans
else
    echo "🔍 Cache-aware build (use --rebuild for a clean build)..."
    dc_run up -d --build --remove-orphans
fi

# ── Kafka topic setup ─────────────────────────────────────────────────────────
# Topics are created by the redpanda-init service (RF=2, 6 partitions).
# Fallback: create topics if using an external broker override.
SOURCE_TOPIC="${SOURCE_TOPIC:-source-events}"
TARGET_TOPIC="${TARGET_TOPIC:-target-events}"

if [ -n "$KAFKA_BROKERS_OVERRIDE" ]; then
    echo "⏳ External Kafka — ensuring topics exist..."
    BROKER=$(echo "$KAFKA_BROKERS_OVERRIDE" | cut -d, -f1)
    docker run --rm --network=host redpandadata/redpanda:v24.2.7 \
        rpk topic create "$SOURCE_TOPIC" "$TARGET_TOPIC" --brokers "$BROKER" 2>/dev/null || true
    echo "   Kafka topics ready: $SOURCE_TOPIC, $TARGET_TOPIC"
fi

# ── Wait for healthy ──────────────────────────────────────────────────────────
echo "⏳ Waiting for all services to become healthy..."
SERVICES="rre-redpanda-0 rre-redpanda-1 rre-redpanda-2 rre-clickhouse rre-postgres rre-app-1 rre-app-2 rre-sre-agent-1 rre-sre-agent-2"
if [ "$OBS_MODE" = true ]; then
    SERVICES="$SERVICES rre-otel-collector rre-signoz"
fi
MAX_ATTEMPTS=90
ATTEMPT=1
while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    ALL_HEALTHY=true
    for c in $SERVICES; do
        STATUS=$(docker inspect --format \
            '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
            "$c" 2>/dev/null || echo "missing")
        if [ "$STATUS" != "healthy" ] && [ "$STATUS" != "none" ]; then
            ALL_HEALTHY=false
        fi
    done
    if [ "$ALL_HEALTHY" = true ]; then
        echo "✅ All services are healthy!"
        break
    fi
    echo "   (Attempt $ATTEMPT/$MAX_ATTEMPTS) waiting..."
    sleep 3
    ATTEMPT=$((ATTEMPT + 1))
done

if [ $ATTEMPT -gt $MAX_ATTEMPTS ]; then
    echo "⚠️  Some services are taking longer than expected."
    echo "   Check with: $DC -f $COMPOSE_FILE ps"
fi

echo "----------------------------------------------------------"
echo "✅ Stack is running in the background."
echo ""
echo "🖥️  Frontend UI:    http://localhost:$FRONTEND_PORT"
echo "⚙️  Rules Engine:   http://localhost:$APP_PORT/health"
echo "🛡️  SRE Agent:      http://localhost:$SRE_PORT"
if [ "$OBS_MODE" = true ]; then
echo "🔭 SigNoz UI:      http://localhost:$SIGNOZ_PORT"
fi
if [ "$OBS_MODE" = true ]; then
echo "🔍 Logs:           $DC -f $COMPOSE_FILE -f $COMPOSE_OBS logs -f"
else
echo "🔍 Logs:           $DC -f $COMPOSE_FILE logs -f"
fi
echo "🛑 Teardown:       ./deploy/run.sh --down"
echo "----------------------------------------------------------"

if [ "$FOLLOW_LOGS" = true ]; then
    echo "📜 Following logs (Ctrl+C to detach)..."
    dc_run logs -f
fi
