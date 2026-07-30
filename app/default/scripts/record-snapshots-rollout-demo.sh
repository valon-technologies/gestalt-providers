#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
LOCAL_DEV_URL="${LOCAL_DEV_URL:-http://127.0.0.1:3107}"
SIGNAL_DIR="$HOME/.cache/gestalt-snapshots-rollout-demo"
CLIP=/tmp/g-issues-snapshots-rollout-clip.mov
SCAP=/tmp/SwiftCapture/.build/release/SwiftCapture
DURATION_MS="${DURATION_MS:-18000}"

rm -f "$CLIP" "$SIGNAL_DIR/READY" "$SIGNAL_DIR/GO"
: > /tmp/g-issues-snapshots-rollout-driver.log

LOCAL_DEV_URL="$LOCAL_DEV_URL" npx playwright test e2e/record-snapshots-rollout-demo.spec.ts \
  --project record-demo \
  >> /tmp/g-issues-snapshots-rollout-driver.log 2>&1 &
DRIVER_PID=$!

for i in $(seq 1 120); do
  test -f "$SIGNAL_DIR/READY" && break
  sleep 0.5
done
test -f "$SIGNAL_DIR/READY" || { echo "driver never ready"; cat /tmp/g-issues-snapshots-rollout-driver.log; exit 1; }

"$SCAP" --app "Google Chrome for Testing" --duration "$DURATION_MS" --fps 60 --force --output "$CLIP" 2>&1 | tee /tmp/g-issues-snapshots-rollout-scap.log &
SCAP_PID=$!
sleep 0.3
touch "$SIGNAL_DIR/GO"
wait "$SCAP_PID"
wait "$DRIVER_PID" || { cat /tmp/g-issues-snapshots-rollout-driver.log; exit 1; }
ls -la "$CLIP"
echo DONE
