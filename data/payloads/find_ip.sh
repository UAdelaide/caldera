#!/bin/bash
set -euo pipefail

NET=${NET:-192.168.40}
START=${START:-60}
END=${END:-66}
PROXY_URL=${PROXY_URL:-"http://localhost:1232/"}
CONTENT_TYPE_HEADER=${CONTENT_TYPE_HEADER:-'Content-Type: application/json'}
CVE_ID=${CVE_ID:-"cve-0"}
TIMESTAMP=$(date +%s)
EXPLOITATION_DIFFICULTY=${EXPLOITATION_DIFFICULTY:-"0.1"}

post_via_proxy () {
  local url="$1"
  curl -s -X POST "$PROXY_URL" \
       -H "$CONTENT_TYPE_HEADER" \
       -d "{\"endpoint\":\"$url\",\"method\":\"GET\",\"headers\":\"\"}"
}

for i in $(seq "$START" "$END"); do
  ip="$NET.$i"
  root_url="http://$ip/"

  resp="$(post_via_proxy "$root_url")" || resp=""
  if [[ -n "$resp" ]]; then

    resp_clean="$(printf '%s' "$resp" | tr -d '\r')"
    expected=$'<pre>\n<a href="1.0">1.0</a>\n<a href="latest/">latest/</a>\n</pre>\n'

    if [[ "$resp_clean" == "$expected" || "$resp_clean" == "${expected%$'\n'}" ]]; then
      echo "{\"target_ip\": \"$ip\", \"success\": \"true\", \"cve_id\": \"$CVE_ID\", \"timestamp\": \"$TIMESTAMP\", \"exploitation_difficulty\": \"$EXPLOITATION_DIFFICULTY\"}"
      exit 0
    fi
  fi
  sleep 2
done

# if we reach here, nothing matched
echo "{\"target_ip\": \"\", \"success\": \"false\", \"cve_id\": \"$CVE_ID\", \"timestamp\": \"$TIMESTAMP\", \"exploitation_difficulty\": \"$EXPLOITATION_DIFFICULTY\"}"
exit 0