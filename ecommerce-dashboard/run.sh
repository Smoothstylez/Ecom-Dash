#!/usr/bin/env bash
set -euo pipefail

# ────────────────────────────────────────────────────────
# HA OS Add-on Entrypoint
# ────────────────────────────────────────────────────────
# /data/  = HA persistent add-on data
# /app/   = application code inside container
#
# Strategy:
#   1. Create persistent directories
#   2. Seed DB/storage on first start
#   3. Symlink /app/data and /app/storage to persistent paths
#   4. Read HA options from /data/options.json
#   5. Export all required env vars
#   6. Start uvicorn
# ────────────────────────────────────────────────────────

PERSISTENT_ROOT="/data"
PERSISTENT_DB="${PERSISTENT_ROOT}/db"
PERSISTENT_STORAGE="${PERSISTENT_ROOT}/storage"
OPTIONS_FILE="${PERSISTENT_ROOT}/options.json"

echo "[add-on] Starting E-Commerce Dashboard..."

# ── 1. Create persistent directories ──────────────────
mkdir -p "${PERSISTENT_DB}/sources/shopify"
mkdir -p "${PERSISTENT_DB}/sources/kaufland"
mkdir -p "${PERSISTENT_DB}/sources/bookkeeping"
mkdir -p "${PERSISTENT_DB}/sources/ebay"
mkdir -p "${PERSISTENT_DB}/restore_safety"
mkdir -p "${PERSISTENT_STORAGE}/documents"
mkdir -p "${PERSISTENT_STORAGE}/invoices"

# ── 2. Seed data on first start ───────────────────────
if [ ! -f "${PERSISTENT_DB}/.seeded" ]; then
    echo "[add-on] First start detected — seeding databases..."

    cp -n /app/data-seed/combined.sqlite3                       "${PERSISTENT_DB}/"                       2>/dev/null || true
    cp -n /app/data-seed/sources/shopify/shopify_data.sqlite3   "${PERSISTENT_DB}/sources/shopify/"      2>/dev/null || true
    cp -n /app/data-seed/sources/kaufland/kaufland_data.sqlite3 "${PERSISTENT_DB}/sources/kaufland/"     2>/dev/null || true
    cp -n /app/data-seed/sources/bookkeeping/dashboard.sqlite3  "${PERSISTENT_DB}/sources/bookkeeping/"  2>/dev/null || true
    cp -n /app/data-seed/sources/ebay/ebay_data.sqlite3         "${PERSISTENT_DB}/sources/ebay/"         2>/dev/null || true

    cp -rn /app/storage-seed/documents/. "${PERSISTENT_STORAGE}/documents/" 2>/dev/null || true
    cp -rn /app/storage-seed/invoices/.  "${PERSISTENT_STORAGE}/invoices/"  2>/dev/null || true

    touch "${PERSISTENT_DB}/.seeded"
    echo "[add-on] Seed complete."
else
    echo "[add-on] Persistent data already exists — skipping seed."
fi

# ── 3. Symlink app paths to persistent volume ─────────
rm -rf /app/data /app/storage
ln -sfn "${PERSISTENT_DB}" /app/data
ln -sfn "${PERSISTENT_STORAGE}" /app/storage

echo "[add-on] Symlinks created:"
echo "         /app/data    -> ${PERSISTENT_DB}"
echo "         /app/storage -> ${PERSISTENT_STORAGE}"

# ── 4. Export fixed app paths ─────────────────────────
export COMBINED_DB_PATH="${PERSISTENT_DB}/combined.sqlite3"
export SHOPIFY_DB_PATH="${PERSISTENT_DB}/sources/shopify/shopify_data.sqlite3"
export KAUFLAND_DB_PATH="${PERSISTENT_DB}/sources/kaufland/kaufland_data.sqlite3"
export BOOKKEEPING_DB_PATH="${PERSISTENT_DB}/sources/bookkeeping/dashboard.sqlite3"
export EBAY_DB_PATH="${PERSISTENT_DB}/sources/ebay/ebay_data.sqlite3"

export STORAGE_INVOICES_DIR="${PERSISTENT_STORAGE}/invoices"
export STORAGE_DOCUMENTS_DIR="${PERSISTENT_STORAGE}/documents"

# Disable bootstrap sync in HA OS unless you explicitly re-enable it later
export AUTO_SYNC_ON_STARTUP=0

# ── 5. Read HA options → env vars ─────────────────────
if [ -f "${OPTIONS_FILE}" ]; then
    echo "[add-on] Reading options from ${OPTIONS_FILE}..."

    export SHOPIFY_CLIENT_ID="$(
        python3 -c "import json; o=json.load(open('${OPTIONS_FILE}')); print(o.get('shopify_client_id',''))" 2>/dev/null || echo ''
    )"
    export SHOPIFY_CLIENT_SECRET="$(
        python3 -c "import json; o=json.load(open('${OPTIONS_FILE}')); print(o.get('shopify_client_secret',''))" 2>/dev/null || echo ''
    )"
    export SHOPIFY_SHOP_DOMAIN="$(
        python3 -c "import json; o=json.load(open('${OPTIONS_FILE}')); print(o.get('shopify_shop_domain',''))" 2>/dev/null || echo ''
    )"

    export SHOP_CLIENT_KEY="$(
        python3 -c "import json; o=json.load(open('${OPTIONS_FILE}')); print(o.get('shop_client_key',''))" 2>/dev/null || echo ''
    )"
    export SHOP_SECRET_KEY="$(
        python3 -c "import json; o=json.load(open('${OPTIONS_FILE}')); print(o.get('shop_secret_key',''))" 2>/dev/null || echo ''
    )"

    export LIVE_SYNC_BACKGROUND_ENABLED="$(
        python3 -c "import json; o=json.load(open('${OPTIONS_FILE}')); print('1' if o.get('live_sync_enabled', False) else '0')" 2>/dev/null || echo '0'
    )"
    export LIVE_SYNC_INTERVAL_SECONDS="$(
        python3 -c "import json; o=json.load(open('${OPTIONS_FILE}')); print(o.get('live_sync_interval_seconds', 300))" 2>/dev/null || echo '300'
    )"

    echo "[add-on] Options loaded."
else
    echo "[add-on] No options file found — running with defaults."
fi

# ── 6. Start server ───────────────────────────────────
echo "[add-on] Starting uvicorn on 0.0.0.0:8012..."
exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8012