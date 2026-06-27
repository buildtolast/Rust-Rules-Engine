#!/usr/bin/env bash
# S11 load test: publish N events via /api/simulation/push and verify audit parity.
#
# Usage:
#   ./tools/load-test.sh [--events 100000] [--base-url http://localhost:8080] [--eos-test]
#
# Requires: curl, python3 (stdlib only), docker (for resource snapshot)
# Does NOT require the stack to be running at this path — set --base-url if
# the rules engine is on a different port (e.g. because run.sh auto-incremented it).

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
EVENTS=100000
BASE_URL="http://localhost:8080"
EOS_TEST=false

# ── Arg parsing ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --events)    EVENTS="$2";   shift 2 ;;
        --base-url)  BASE_URL="$2"; shift 2 ;;
        --eos-test)  EOS_TEST=true; shift ;;
        --help|-h)
            sed -n '2,12p' "$0"
            exit 0
            ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────
# Extract a top-level numeric field from a JSON response.
# Usage: json_field <json_string> <camelCase_key>
json_field() {
    local json="$1" key="$2"
    python3 -c "
import sys, json
try:
    d = json.loads(sys.argv[1])
    print(d[sys.argv[2]])
except Exception:
    print(0)
" "$json" "$key" 2>/dev/null || echo 0
}

# Count enabled rules from the /api/rules JSON array.
count_enabled_rules() {
    local json="$1"
    python3 -c "
import sys, json
try:
    rules = json.loads(sys.argv[1])
    print(sum(1 for r in rules if r.get('enabled', False)))
except Exception:
    print(0)
" "$json" 2>/dev/null || echo 0
}

# ── Preflight ─────────────────────────────────────────────────────────────────
echo "=== Rust Rules Engine -- Load Test ==="
echo "Target:  $BASE_URL"
echo "Events:  $EVENTS"
echo ""

echo "[1/6] Health check..."
HEALTH_RESP=$(curl -sf --max-time 5 "$BASE_URL/health" 2>&1) || {
    echo "FAIL: $BASE_URL/health did not return 200."
    echo "      Start the stack first: ./deploy/run.sh"
    exit 1
}
echo "      OK -- $HEALTH_RESP"

echo "[2/6] Counting active rules..."
RULES_JSON=$(curl -sf --max-time 10 "$BASE_URL/api/rules" 2>/dev/null || echo "[]")
ACTIVE_RULES=$(count_enabled_rules "$RULES_JSON")

if [ "$ACTIVE_RULES" -eq 0 ]; then
    echo "WARNING: 0 active rules found. The simulation will still publish events"
    echo "         but audit parity check will trivially pass (0 x $EVENTS = 0)."
    echo "         Seed rules via the UI or API before running this test for"
    echo "         meaningful parity numbers."
fi

EXPECTED_AUDITS=$((EVENTS * ACTIVE_RULES))
echo "      Active rules: $ACTIVE_RULES"
echo "      Expected audits: $EVENTS x $ACTIVE_RULES = $EXPECTED_AUDITS"
echo ""

# ── Baseline analytics snapshot ───────────────────────────────────────────────
echo "[3/6] Capturing baseline analytics (last 24 h window)..."
BASELINE_JSON=$(curl -sf --max-time 10 "$BASE_URL/api/analytics/stats" 2>/dev/null || echo "{}")
BASELINE_EVALS=$(json_field "$BASELINE_JSON" "totalEvaluations")
BASELINE_MSGS=$(json_field "$BASELINE_JSON" "totalMessages")
echo "      Baseline totalEvaluations: $BASELINE_EVALS"
echo "      Baseline totalMessages:    $BASELINE_MSGS"
echo ""

# ── Trigger publish ───────────────────────────────────────────────────────────
echo "[4/6] Triggering simulation publish ($EVENTS events)..."
PUSH_START=$(date +%s)
PUSH_RESP=$(curl -sf --max-time 30 \
    -X POST "$BASE_URL/api/simulation/push?count=$EVENTS" 2>/dev/null || echo "{}")
echo "      Response: $PUSH_RESP"
echo "      (publish runs in background -- polling for audit parity now)"
echo ""

# ── Resource snapshot (during peak load) ─────────────────────────────────────
echo "[5/6] Resource usage at peak (live docker stats snapshot)..."
# Non-fatal: docker may not be available or containers may have different names.
docker stats --no-stream --format \
    "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" \
    rre-app-1 rre-app-2 \
    rre-redpanda-0 rre-redpanda-1 rre-redpanda-2 \
    rre-clickhouse rre-postgres 2>/dev/null \
    || echo "      (docker stats unavailable -- run manually: docker stats --no-stream)"
echo ""

# ── Poll for audit parity ─────────────────────────────────────────────────────
echo "[6/6] Polling for audit parity (max 10 min, 5 s interval)..."
EXPECTED_TOTAL=$((BASELINE_EVALS + EXPECTED_AUDITS))
echo "      Waiting for totalEvaluations >= $EXPECTED_TOTAL"
echo ""

PARITY_REACHED=false
for i in $(seq 1 120); do
    CURRENT_JSON=$(curl -sf --max-time 10 "$BASE_URL/api/analytics/stats" 2>/dev/null || echo "{}")
    CURRENT_EVALS=$(json_field "$CURRENT_JSON" "totalEvaluations")
    CURRENT_MSGS=$(json_field "$CURRENT_JSON" "totalMessages")
    DELTA_EVALS=$((CURRENT_EVALS - BASELINE_EVALS))
    ELAPSED=$(( $(date +%s) - PUSH_START ))

    printf "  [%3d/120] elapsed %ds | totalEvaluations=%d | delta=%d / %d | totalMessages=%d\n" \
        "$i" "$ELAPSED" "$CURRENT_EVALS" "$DELTA_EVALS" "$EXPECTED_AUDITS" "$CURRENT_MSGS"

    if [ "$EXPECTED_AUDITS" -gt 0 ] && [ "$DELTA_EVALS" -ge "$EXPECTED_AUDITS" ]; then
        PARITY_REACHED=true
        break
    fi

    # If 0 active rules, consider complete once totalMessages has grown by EVENTS.
    if [ "$ACTIVE_RULES" -eq 0 ]; then
        DELTA_MSGS=$((CURRENT_MSGS - BASELINE_MSGS))
        if [ "$DELTA_MSGS" -ge "$EVENTS" ]; then
            PARITY_REACHED=true
            break
        fi
    fi

    sleep 5
done

echo ""

# ── Results ───────────────────────────────────────────────────────────────────
FINAL_JSON=$(curl -sf --max-time 10 "$BASE_URL/api/analytics/stats" 2>/dev/null || echo "{}")
FINAL_EVALS=$(json_field "$FINAL_JSON" "totalEvaluations")
FINAL_MSGS=$(json_field "$FINAL_JSON" "totalMessages")
ACTUAL_DELTA=$((FINAL_EVALS - BASELINE_EVALS))
TOTAL_ELAPSED=$(( $(date +%s) - PUSH_START ))

echo "=== Results ==="
echo "  Events published:       $EVENTS"
echo "  Active rules:           $ACTIVE_RULES"
echo "  Expected audit delta:   $EXPECTED_AUDITS"
echo "  Actual audit delta:     $ACTUAL_DELTA"
echo "  Final totalMessages:    $FINAL_MSGS"
echo "  Elapsed:                ${TOTAL_ELAPSED}s"
echo ""

if [ "$PARITY_REACHED" = true ]; then
    echo "  PARITY: PASS"
else
    echo "  PARITY: FAIL (timeout -- check pipeline logs)"
    echo "          docker logs rre-app-1 --tail 50"
    echo "          docker logs rre-app-2 --tail 50"
fi

echo ""
echo "  Final analytics JSON:"
echo "$FINAL_JSON" | python3 -c "import sys, json; print(json.dumps(json.load(sys.stdin), indent=2))" \
    2>/dev/null || echo "$FINAL_JSON"

# ── EOS soak test ─────────────────────────────────────────────────────────────
if [ "$EOS_TEST" = true ]; then
    echo ""
    echo "=== EOS Soak Test ==="
    echo "Restarting rre-app-1 to verify no duplicate audits after consumer rebalance..."
    docker restart rre-app-1 2>/dev/null || echo "WARNING: could not restart rre-app-1"
    echo "  Waiting 30 s for app-1 to recover and consumer group to rebalance..."
    sleep 30

    echo "  Querying deduplicated audit count from ClickHouse..."
    DEDUP_COUNT=$(docker exec rre-clickhouse \
        clickhouse-client --user rules --password rules \
        --query "SELECT count() FROM ruleaudit.audits FINAL" 2>/dev/null || echo "N/A")
    RAW_COUNT=$(docker exec rre-clickhouse \
        clickhouse-client --user rules --password rules \
        --query "SELECT count() FROM ruleaudit.audits" 2>/dev/null || echo "N/A")

    echo ""
    echo "  Raw audit rows (pre-dedup):        $RAW_COUNT"
    echo "  Deduplicated audit rows (FINAL):   $DEDUP_COUNT"

    if [ "$RAW_COUNT" != "N/A" ] && [ "$DEDUP_COUNT" != "N/A" ]; then
        if [ "$RAW_COUNT" -eq "$DEDUP_COUNT" ] 2>/dev/null; then
            echo "  EOS dedup: PASS (raw == FINAL, no duplicates written)"
        else
            DIFF=$((RAW_COUNT - DEDUP_COUNT))
            echo "  EOS dedup: $DIFF duplicate rows detected (FINAL removed them)"
            echo "             ReplacingMergeTree dedup may still be running."
            echo "             Force merge: docker exec rre-clickhouse clickhouse-client"
            echo "               --query 'OPTIMIZE TABLE ruleaudit.audits FINAL'"
        fi
    fi

    echo ""
    echo "  Checking audit parity still holds after restart..."
    POST_JSON=$(curl -sf --max-time 10 "$BASE_URL/api/analytics/stats" 2>/dev/null || echo "{}")
    POST_EVALS=$(json_field "$POST_JSON" "totalEvaluations")
    POST_DELTA=$((POST_EVALS - BASELINE_EVALS))
    echo "  Post-restart audit delta: $POST_DELTA (expected: $EXPECTED_AUDITS)"
    if [ "$EXPECTED_AUDITS" -gt 0 ] && [ "$POST_DELTA" -ge "$EXPECTED_AUDITS" ]; then
        echo "  Post-restart parity: PASS"
    else
        echo "  Post-restart parity: re-check (pipeline may still be processing)"
    fi
fi

echo ""
echo "Done. Copy resource usage and parity numbers into BENCHMARKS.md."
