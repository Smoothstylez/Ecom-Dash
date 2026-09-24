#!/usr/bin/env bash
# Eingangsrechnung einlesen, ablegen und freigeben.
#
#   platform-invoice.sh parse  <beleg.pdf> [csv]
#   platform-invoice.sh draft  <parsed.json> [document_id]
#   platform-invoice.sh approve <invoice_id>
#
# parse legt nichts an, draft bucht nichts, approve ist der einzige Weg ins
# Ledger. Siehe docs/platform-invoice-parsing.md und docs/dashboard-backend-api.md.
set -euo pipefail

BASE="${ECOM_DASH_API:-http://127.0.0.1:8012}"
TOKEN="${ECOM_DASH_ADMIN_TOKEN:-${APP_ADMIN_TOKEN:-}}"
if [ -z "$TOKEN" ]; then
  echo "APP_ADMIN_TOKEN fehlt" >&2
  exit 1
fi
AUTH=(-H "X-Admin-Token: $TOKEN" -H "Content-Type: application/json")
CMD="${1:-}"; shift || true

case "$CMD" in
  parse)
    PDF="${1:?PDF-Pfad fehlt}"; CSV="${2:-}"
    PDF_TEXT="$(pdftotext -layout "$PDF" -)"
    CSV_TEXT=""
    [ -n "$CSV" ] && CSV_TEXT="$(cat "$CSV")"
    python3 - "$PDF_TEXT" "$CSV_TEXT" <<'PY'
import json, sys
print(json.dumps({"pdf_text": sys.argv[1], "csv_text": sys.argv[2] or None}))
PY
    | curl -sS "${AUTH[@]}" -X POST "$BASE/api/bookings/monthly-invoices/parse" -d @- | python3 -m json.tool
    ;;
  draft)
    PARSED="${1:?parsed.json fehlt}"; DOC="${2:-}"
    python3 - "$PARSED" "$DOC" <<'PY'
import json, sys
payload = json.load(open(sys.argv[1]))
print(json.dumps({"parsed": payload, "document_id": sys.argv[2] or None}))
PY
    | curl -sS "${AUTH[@]}" -X POST "$BASE/api/bookings/monthly-invoices/draft" -d @- | python3 -m json.tool
    ;;
  approve)
    ID="${1:?invoice_id fehlt}"
    curl -sS "${AUTH[@]}" -X POST "$BASE/api/bookings/monthly-invoices/$ID/approve" \
      -d '{"create_missing_bookings": true}' | python3 -m json.tool
    ;;
  *)
    echo "Aufruf: $0 parse|draft|approve ..." >&2
    exit 1
    ;;
esac
