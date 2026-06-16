#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/dashboard-api/_lib.sh"

marketplace="${1:-}"
order_id="${2:-}"
file_path="${3:-}"
purchase_cost_eur="${4:-}"
purchase_currency="${5:-EUR}"
supplier_name="${6:-}"
notes="${7:-}"

dashboard_require_arg "$marketplace" "marketplace"
dashboard_require_arg "$order_id" "order_id"
dashboard_require_arg "$file_path" "file_path"
dashboard_require_file "$file_path"

marketplace_enc="$(dashboard_urlencode "$marketplace")"
order_id_enc="$(dashboard_urlencode "$order_id")"
base_url="$(dashboard_api_base_url)"

cmd=(dashboard_api_curl -X POST)
cmd+=(-F "file=@${file_path}")
if [[ -n "$purchase_cost_eur" ]]; then
  cmd+=(-F "purchase_cost_eur=${purchase_cost_eur}")
fi
if [[ -n "$purchase_currency" ]]; then
  cmd+=(-F "purchase_currency=${purchase_currency}")
fi
if [[ -n "$supplier_name" ]]; then
  cmd+=(-F "supplier_name=${supplier_name}")
fi
if [[ -n "$notes" ]]; then
  cmd+=(-F "notes=${notes}")
fi
cmd+=("$base_url/api/orders/${marketplace_enc}/${order_id_enc}/invoice")

"${cmd[@]}"
