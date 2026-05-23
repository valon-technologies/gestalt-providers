#!/usr/bin/env bash
set -euo pipefail

# Verify apps/* release metadata URLs in provider-index.yaml return HTTP 200.
# Run after batch-tagging apps/{name}/v{version} releases.

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
INDEX="${ROOT}/provider-index.yaml"
VERSION="${1:-0.0.1-alpha.1}"

cd "${ROOT}"

export INDEX
mapfile -t URLS < <(
  python3 - <<'PY'
import os
import re
from pathlib import Path

text = Path(os.environ["INDEX"]).read_text()
for match in re.finditer(r'metadata: "([^"]+/apps/[^"]+/provider-release\.yaml)"', text):
    print(match.group(1))
PY
)

if ((${#URLS[@]} == 0)); then
  echo "no apps release metadata URLs found in ${INDEX}" >&2
  exit 1
fi

failed=0
for url in "${URLS[@]}"; do
  code="$(curl -s -o /dev/null -w '%{http_code}' "$url" || true)"
  if [[ "$code" != "200" ]]; then
    echo "FAIL $code $url" >&2
    failed=1
  else
    echo "OK   $url"
  fi
done

if ((failed != 0)); then
  echo "one or more release metadata URLs are missing; batch-tag apps/*/v${VERSION} before merging index changes" >&2
  exit 1
fi

echo "all ${#URLS[@]} apps release metadata URLs verified"
