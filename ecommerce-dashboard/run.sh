#!/usr/bin/env bash
set -euo pipefail

# ────────────────────────────────────────────────────────
# HA OS Add-on Entrypoint
# ────────────────────────────────────────────────────────
# /data/  = HA persistent volume (survives restarts/rebuilds)
# /app/   = application code (rebuilt with each add-on update)
#
# Strategy:
#   1. On FIRST start: copy seed DBs + storage into /data/
#   2. Symlink /app/data → /data/db  and  /app/storage → /data/storage
#   3. Read HA options from /data/options.json → export as env vars
#   4. Launch uvicorn
# ────────────────────────────────────────────────────────

PERSISTENT_DB="/data/db"
PERSISTENT_STORAGE="/data/storage"

echo "[add-on] Starting E-Commerce Dashboard..."

# ── 1. Create persistent directories ──────────────────
mkdir -p "$PERSISTENT_DB/sources/shopify"
mkdir -p "$PERSISTENT_DB/sources/kaufland"
mkdir -p "$PERSISTENT_DB/sources/bookkeeping"
mkdir -p "$PERSISTENT_DB/sources/ebay"
mkdir -p "$PERSISTENT_DB/restore_safety"
mkdir -p "$PERSISTENT_STORAGE/documents"
mkdir -p "$PERSISTENT_STORAGE/invoices"

# ── 2. Seed data on first start ───────────────────────
if [ ! -f "$PERSISTENT_DB/.seeded" ]; then
    echo "[add-on] First start detected — seeding databases..."
    # Copy all seed DB files (don't overwrite if somehow existing)
    cp -n /app/data-seed/combined.sqlite3                         "$PERSISTENT_DB/"                          2>/dev/null || true
    cp -n /app/data-seed/sources/shopify/shopify_data.sqlite3     "$PERSISTENT_DB/sources/shopify/"          2>/dev/null || true
    cp -n /app/data-seed/sources/kaufland/kaufland_data.sqlite3   "$PERSISTENT_DB/sources/kaufland/"         2>/dev/null || true
    cp -n /app/data-seed/sources/bookkeeping/dashboard.sqlite3    "$PERSISTENT_DB/sources/bookkeeping/"      2>/dev/null || true
    cp -n /app/data-seed/sources/ebay/ebay_data.sqlite3           "$PERSISTENT_DB/sources/ebay/"             2>/dev/null || true

    # Copy seed storage files
    cp -rn /app/storage-seed/documents/* "$PERSISTENT_STORAGE/documents/" 2>/dev/null || true
    cp -rn /app/storage-seed/invoices/*  "$PERSISTENT_STORAGE/invoices/"  2>/dev/null || true

    touch "$PERSISTENT_DB/.seeded"
    echo "[add-on] Seed complete."
else
    echo "[add-on] Persistent data already exists — skipping seed."
fi

# ── 3. Symlink app paths to persistent volume ─────────
# Remove the app-embedded data/storage dirs and replace with symlinks
rm -rf /app/data /app/storage
ln -sf "$PERSISTENT_DB"      /app/data
ln -sf "$PERSISTENT_STORAGE" /app/storage
echo "[add-on] Symlinks created: /app/data → $PERSISTENT_DB, /app/storage → $PERSISTENT_STORAGE"

# ── 4. Read HA options → env vars ─────────────────────
OPTIONS_FILE="/data/options.json"
if [ -f "$OPTIONS_FILE" ]; then
    echo "[add-on] Reading options from $OPTIONS_FILE..."

    # Shopify credentials
    SHOPIFY_API_KEY=$(python3 -c "import json; o=json.load(open('$OPTIONS_FILE')); print(o.get('shopify_api_key',''))" 2>/dev/null || echo "")
    SHOPIFY_API_SECRET=$(python3 -c "import json; o=json.load(open('$OPTIONS_FILE')); print(o.get('shopify_api_secret',''))" 2>/dev/null || echo "")
    SHOPIFY_SHOP_DOMAIN=$(python3 -c "import json; o=json.load(open('$OPTIONS_FILE')); print(o.get('shopify_shop_domain',''))" 2>/dev/null || echo "")

    # Kaufland credentials
    KAUFLAND_CLIENT_KEY=$(python3 -c "import json; o=json.load(open('$OPTIONS_FILE')); print(o.get('kaufland_client_key',''))" 2>/dev/null || echo "")
    KAUFLAND_SECRET_KEY=$(python3 -c "import json; o=json.load(open('$OPTIONS_FILE')); print(o.get('kaufland_secret_key',''))" 2>/dev/null || echo "")

    # Live sync
    LIVE_SYNC_ENABLED=$(python3 -c "import json; o=json.load(open('$OPTIONS_FILE')); print('1' if o.get('live_sync_enabled', False) else '0')" 2>/dev/null || echo "0")
    LIVE_SYNC_INTERVAL=$(python3 -c "import json; o=json.load(open('$OPTIONS_FILE')); print(o.get('live_sync_interval_seconds', 300))" 2>/dev/null || echo "300")

    [ -n "$SHOPIFY_API_KEY" ]    && export SHOPIFY_API_KEY
    [ -n "$SHOPIFY_API_SECRET" ] && export SHOPIFY_API_SECRET
    [ -n "$SHOPIFY_SHOP_DOMAIN" ] && export SHOPIFY_SHOP_DOMAIN
    [ -n "$KAUFLAND_CLIENT_KEY" ] && export KAUFLAND_CLIENT_KEY
    [ -n "$KAUFLAND_SECRET_KEY" ] && export KAUFLAND_SECRET_KEY
    export LIVE_SYNC_BACKGROUND_ENABLED="$LIVE_SYNC_ENABLED"
    export LIVE_SYNC_INTERVAL_SECONDS="$LIVE_SYNC_INTERVAL"

    echo "[add-on] Options loaded."
else
    echo "[add-on] No options file found — running with defaults."
fi

# ── 5. Disable bootstrap sync (no sibling projects on HA OS) ──
export AUTO_SYNC_ON_STARTUP=0

# ── 6. Launch uvicorn ─────────────────────────────────
echo "[add-on] Starting uvicorn on 0.0.0.0:8012..."
exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8012
