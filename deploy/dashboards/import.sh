#!/usr/bin/env bash
# Import the RRE main dashboard into a running SigNoz instance.
#
# Usage:
#   SIGNOZ_EMAIL=you@example.com SIGNOZ_PASSWORD=yourpass ./import.sh
#   SIGNOZ_HOST=http://localhost:3301 ./import.sh   # override host
#
# The script:
#   1. Logs in to get a JWT access token
#   2. POSTs the dashboard JSON to /api/v1/dashboards
#   3. Prints the created dashboard UUID

set -euo pipefail

HOST="${SIGNOZ_HOST:-http://localhost:3301}"
EMAIL="${SIGNOZ_EMAIL:-}"
PASSWORD="${SIGNOZ_PASSWORD:-}"
DASHBOARD_FILE="$(dirname "$0")/rre-main.json"

if [[ -z "$EMAIL" || -z "$PASSWORD" ]]; then
  echo "ERROR: set SIGNOZ_EMAIL and SIGNOZ_PASSWORD before running this script."
  exit 1
fi

echo "Logging in to SigNoz at $HOST ..."
TOKEN=$(curl -sf -X POST "$HOST/api/v1/login" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['accessJwt'])")

if [[ -z "$TOKEN" ]]; then
  echo "ERROR: login failed — check credentials."
  exit 1
fi

echo "Importing dashboard from $DASHBOARD_FILE ..."
RESULT=$(curl -sf -X POST "$HOST/api/v1/dashboards" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d @"$DASHBOARD_FILE")

UUID=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('data',{}).get('uuid','(unknown)'))" 2>/dev/null || echo "(check output)")
echo "Done. Dashboard UUID: $UUID"
echo "Open: $HOST/dashboard/$UUID"
