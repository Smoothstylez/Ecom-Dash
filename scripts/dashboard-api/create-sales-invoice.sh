#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/dashboard-api/_lib.sh"

marketplace="${1:-}"
order_id="${2:-}"
template_key="${3:-}"
preview_only="false"

if [[ "$marketplace" == "--help" || "$marketplace" == "-h" ]]; then
  cat <<'EOF'
Usage: create-sales-invoice.sh <marketplace> <order_id> [template_key] [--preview-only]

Flow:
  1. Reads invoice draft
  2. Optionally downloads preview PDF metadata when requested separately by direct call
  3. Creates invoice unless --preview-only is used
EOF
  exit 0
fi

for arg in "$@"; do
  if [[ "$arg" == "--preview-only" ]]; then
    preview_only="true"
  fi
done

if [[ "$marketplace" == "--preview-only" ]]; then
  marketplace=""
fi
if [[ "$order_id" == "--preview-only" ]]; then
  order_id=""
fi
if [[ "$template_key" == "--preview-only" ]]; then
  template_key=""
fi

dashboard_require_arg "$marketplace" "marketplace"
dashboard_require_arg "$order_id" "order_id"

query_args=("marketplace=$marketplace" "order_id=$order_id")
if [[ -n "$template_key" && "$template_key" != "--preview-only" ]]; then
  query_args+=("template_key=$template_key")
fi
query_string="$(dashboard_build_query "${query_args[@]}")"

printf '### Invoice draft\n' >&2
dashboard_api_get "/api/invoices/draft${query_string}"

if [[ "$preview_only" == "true" ]]; then
  exit 0
fi

template_json="null"
if [[ -n "$template_key" && "$template_key" != "--preview-only" ]]; then
  template_json="$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$template_key")"
fi

body="$(python3 - "$marketplace" "$order_id" "$template_json" <<'PY'
import json
import sys

marketplace = sys.argv[1]
order_id = sys.argv[2]
template_raw = sys.argv[3]
payload = {
    "marketplace": marketplace,
    "order_id": order_id,
}
template_key = json.loads(template_raw)
if template_key is not None:
    payload["template_key"] = template_key
print(json.dumps(payload, ensure_ascii=False))
PY
)"

printf '\n### Invoice create\n' >&2
dashboard_api_json POST "/api/invoices" "$body"
