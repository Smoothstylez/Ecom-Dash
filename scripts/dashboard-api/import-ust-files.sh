#!/usr/bin/env bash
# Original PDFs and tax/fee CSVs use the same automatic pipeline as the UI.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_lib.sh"
if [[ "$#" -eq 0 ]]; then
  printf 'Usage: bash %s invoice.pdf [fees.csv] [tax-report.csv]\n' "$0" >&2
  exit 2
fi
args=(-X POST)
for file in "$@"; do
  dashboard_require_file "$file"
  args+=(-F "files=@${file}")
done
# Inspect each item's status/reasons and fee-CSV pairings: paired_review means
# the original PDF was found but review remains; waiting_pdf lists missing IDs.
dashboard_api_curl "${args[@]}" "$(dashboard_api_base_url)/api/ust-report/import"
