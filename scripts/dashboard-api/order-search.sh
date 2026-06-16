#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/dashboard-api/_lib.sh"

marketplace=""
from_date=""
to_date=""
query=""
status=""
limit="100"
offset="0"
hide_canceled="false"
has_purchase_cost="false"
no_purchase_cost="false"
has_invoice="false"
no_invoice="false"
payments=()

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --marketplace) marketplace="${2:-}"; shift 2 ;;
    --from) from_date="${2:-}"; shift 2 ;;
    --to) to_date="${2:-}"; shift 2 ;;
    --query) query="${2:-}"; shift 2 ;;
    --status) status="${2:-}"; shift 2 ;;
    --limit) limit="${2:-}"; shift 2 ;;
    --offset) offset="${2:-}"; shift 2 ;;
    --payment) payments+=("${2:-}"); shift 2 ;;
    --hide-canceled) hide_canceled="true"; shift ;;
    --has-purchase-cost) has_purchase_cost="true"; shift ;;
    --no-purchase-cost) no_purchase_cost="true"; shift ;;
    --has-invoice) has_invoice="true"; shift ;;
    --no-invoice) no_invoice="true"; shift ;;
    -h|--help)
      cat <<'EOF'
Usage: order-search.sh [options]

Options:
  --marketplace shopify|kaufland
  --from YYYY-MM-DD
  --to YYYY-MM-DD
  --query TEXT
  --status TOKEN
  --payment VALUE            repeatable
  --limit N                 default 100
  --offset N                default 0
  --hide-canceled
  --has-purchase-cost
  --no-purchase-cost
  --has-invoice
  --no-invoice
EOF
      exit 0
      ;;
    *)
      printf 'Unknown argument: %s\n' "$1" >&2
      exit 2
      ;;
  esac
done

query_args=(
  "marketplace=$marketplace"
  "from=$from_date"
  "to=$to_date"
  "q=$query"
  "status=$status"
  "limit=$limit"
  "offset=$offset"
)

if [[ "$hide_canceled" == "true" ]]; then
  query_args+=("hide_canceled=true")
fi
if [[ "$has_purchase_cost" == "true" ]]; then
  query_args+=("has_purchase_cost=true")
fi
if [[ "$no_purchase_cost" == "true" ]]; then
  query_args+=("no_purchase_cost=true")
fi
if [[ "$has_invoice" == "true" ]]; then
  query_args+=("has_invoice=true")
fi
if [[ "$no_invoice" == "true" ]]; then
  query_args+=("no_invoice=true")
fi
for payment in "${payments[@]}"; do
  query_args+=("payment=$payment")
done

query_string="$(dashboard_build_query "${query_args[@]}")"
dashboard_api_get "/api/orders${query_string}"
