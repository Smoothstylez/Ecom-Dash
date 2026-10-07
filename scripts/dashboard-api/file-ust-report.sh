#!/usr/bin/env bash
# Finance-only warnings are informational; inspect sections.finance_reconciliation
# separately from actual tax blockers before filing. Never overwrite tax totals
# to force agreement with Finance release dates.
# File (or amend) the monthly USt report and print the resulting revision.
#
# Usage:
#   file-ust-report.sh 2026-02            # file the month
#   file-ust-report.sh 2026-02 --amend    # append an amendment revision
# Missing marketplace tax rows, ambiguous cutoff timestamps and undated
# refunds are hard blockers. Inspect GET /api/ust-report?month=YYYY-MM,
# especially sections.amazon_reconciliation, before resolving a 409.
# Also check INPUT_VAT_ELIGIBILITY_UNRESOLVED and the effective deduction
# annotations; original invoice VAT is not always the deductible VAT.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/dashboard-api/_lib.sh
source "${SCRIPT_DIR}/_lib.sh"

MONTH="${1:-}"
MODE="${2:-file}"
if [[ -z "${MONTH}" ]]; then
  echo "usage: $0 YYYY-MM [--amend]" >&2
  exit 2
fi
if [[ ! "${MONTH}" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
  echo "month must be YYYY-MM" >&2
  exit 2
fi

ACTION="file"
[[ "${MODE}" == "--amend" ]] && ACTION="amend"

dashboard_api_json POST "/api/ust-report/${MONTH}/${ACTION}" '{}'
