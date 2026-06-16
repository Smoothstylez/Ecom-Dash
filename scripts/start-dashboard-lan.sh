#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_DIR="$ROOT_DIR/ecommerce-dashboard"
FRONTEND_DIR="$ROOT_DIR/frontend"
ENV_FILE="$BACKEND_DIR/.env.runtime"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

mkdir -p /tmp/opencode

pkill -f "uvicorn app.main:app --host 0.0.0.0 --port 8012" || true
pkill -f "vite --host 0.0.0.0 --port 5173" || true

(cd "$BACKEND_DIR" && nohup python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8012 \
  > /tmp/opencode/ecomdash-8012-lan.log 2>&1 < /dev/null &) 

(cd "$FRONTEND_DIR" && nohup npm run dev -- --host 0.0.0.0 --port 5173 \
  > /tmp/opencode/ecomdash-5173.log 2>&1 < /dev/null &) 

sleep 3

echo "Backend:  http://0.0.0.0:8012"
echo "Frontend: http://0.0.0.0:5173"
