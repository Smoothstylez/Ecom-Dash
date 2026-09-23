#!/usr/bin/env bash
# File (or amend) the monthly USt report and print the resulting revision.
#
# Usage:
#   file-ust-report.sh 2026-02            # file the month
#   file-ust-report.sh 2026-02 --amend    # append an amendment revision
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

dashboard_api POST "/api/ust-report/${MONTH}/${ACTION}" '{}'
