#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/dashboard-api/_lib.sh"

marketplace="${1:-}"
order_id="${2:-}"
carrier="${3:-}"
tracking_number="${4:-}"

dashboard_require_arg "$marketplace" "marketplace"
dashboard_require_arg "$order_id" "order_id"
dashboard_require_arg "$carrier" "carrier"

marketplace_enc="$(dashboard_urlencode "$marketplace")"
order_id_enc="$(dashboard_urlencode "$order_id")"
temp_body="$(mktemp)"
trap 'rm -f "$temp_body"' EXIT

python3 - "$carrier" "$tracking_number" <<'PY' > "$temp_body"
import json
import sys

carrier = sys.argv[1]
tracking_number = sys.argv[2] if len(sys.argv) > 2 else ""
print(json.dumps({"carrier": carrier, "tracking_number": tracking_number}, ensure_ascii=False))
PY

dashboard_api_json \
  PATCH \
  "/api/orders/${marketplace_enc}/${order_id_enc}/shipment" \
  "$(< "$temp_body")"
