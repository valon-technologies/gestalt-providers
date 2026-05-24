#!/usr/bin/env bash
# Re-push app/* release tags one at a time and wait for success.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

SHA="${SHA:-$(git rev-parse HEAD)}"
SUCCESS_TAGS="${SUCCESS_TAGS:-app/ashby/v0.0.1-alpha.1 app/bigquery/v0.0.1-alpha.1 app/github/v0.0.1-alpha.2 app/notion/v0.0.1-alpha.1 app/trunk/v0.0.1-alpha.1 app/zendesk/v0.0.1-alpha.1}"
LOG="${LOG:-/tmp/retrigger-app-releases.log}"
POLL_SECS="${POLL_SECS:-30}"
MAX_WAIT_SECS="${MAX_WAIT_SECS:-2400}"

is_success_tag() {
  local tag="$1"
  [[ " ${SUCCESS_TAGS} " == *" ${tag} "* ]]
}

wait_for_release() {
  local tag="$1"
  local deadline=$((SECONDS + MAX_WAIT_SECS))
  while ((SECONDS < deadline)); do
    local conclusion
    conclusion="$(gh run list --workflow=release-app.yml --limit 10 \
      --json conclusion,headBranch,status \
      -q ".[] | select(.headBranch==\"${tag}\") | .conclusion" 2>/dev/null | head -1 || true)"
    if [[ "$conclusion" == "success" ]]; then
      return 0
    fi
    if [[ "$conclusion" == "failure" ]]; then
      return 1
    fi
    sleep "$POLL_SECS"
  done
  return 1
}

retrigger_tag() {
  local tag="$1"
  echo "==> ${tag}" | tee -a "$LOG"
  git push origin ":refs/tags/${tag}" 2>/dev/null || true
  git tag -f "${tag}" "${SHA}"
  if ! git push origin "${tag}" --force 2>>"$LOG"; then
    echo "FAILED push ${tag}" | tee -a "$LOG"
    return 1
  fi
  if wait_for_release "${tag}"; then
    echo "OK ${tag}" | tee -a "$LOG"
    SUCCESS_TAGS="${SUCCESS_TAGS} ${tag}"
    return 0
  fi
  echo "FAILED release ${tag}" | tee -a "$LOG"
  return 1
}

: >"$LOG"
failed=0
while IFS= read -r tag; do
  [[ -n "$tag" ]] || continue
  is_success_tag "$tag" && continue
  retrigger_tag "$tag" || failed=$((failed + 1))
done < <(git tag -l 'app/*/v*' | sort)

echo "Done. failed=${failed}" | tee -a "$LOG"
exit "$failed"
