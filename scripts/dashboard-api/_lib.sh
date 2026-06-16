#!/usr/bin/env bash
set -euo pipefail

dashboard_api_base_url() {
  local base_url="${DASHBOARD_BASE_URL:-}"
  if [[ -z "$base_url" ]]; then
    base_url="http://192.168.178.197:8012"
  fi
  printf '%s' "${base_url%/}"
}

dashboard_api_token() {
  if [[ -n "${DASHBOARD_ADMIN_TOKEN:-}" ]]; then
    printf '%s' "$DASHBOARD_ADMIN_TOKEN"
    return 0
  fi
  if [[ -n "${APP_ADMIN_TOKEN:-}" ]]; then
    printf '%s' "$APP_ADMIN_TOKEN"
    return 0
  fi
  return 1
}

dashboard_api_curl() {
  local -a cmd
  cmd=(curl --fail-with-body -sS)
  if token="$(dashboard_api_token 2>/dev/null)"; then
    cmd+=(-H "X-Admin-Token: $token")
  fi
  cmd+=("$@")
  "${cmd[@]}"
}

dashboard_api_json() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local base_url
  base_url="$(dashboard_api_base_url)"

  if [[ -n "$body" ]]; then
    dashboard_api_curl \
      -X "$method" \
      -H "Content-Type: application/json" \
      -d "$body" \
      "$base_url$path"
    return 0
  fi

  dashboard_api_curl -X "$method" "$base_url$path"
}

dashboard_api_get() {
  local path="$1"
  local base_url
  base_url="$(dashboard_api_base_url)"
  dashboard_api_curl "$base_url$path"
}

dashboard_require_arg() {
  local value="$1"
  local name="$2"
  if [[ -z "$value" ]]; then
    printf 'Missing required argument: %s\n' "$name" >&2
    exit 2
  fi
}

dashboard_require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    printf 'File not found: %s\n' "$path" >&2
    exit 2
  fi
}

dashboard_urlencode() {
  python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

dashboard_build_query() {
  if [[ "$#" -eq 0 ]]; then
    return 0
  fi
  python3 - "$@" <<'PY'
import sys
from urllib.parse import urlencode

pairs = []
for raw in sys.argv[1:]:
    if not raw or "=" not in raw:
        continue
    key, value = raw.split("=", 1)
    if value == "":
        continue
    pairs.append((key, value))

query = urlencode(pairs, doseq=True)
if query:
    print(f"?{query}")
PY
}
