#!/bin/bash
set -euo pipefail
export API_PORT=8765
export GESTALT_BASE_URL="http://127.0.0.1:${API_PORT}"
cd "$(dirname "$0")/.."
bun run build
rm -rf out && cp -R dist out

node scripts/serve-mock.mjs >> /tmp/versions-mock-server.log 2>&1 &
MOCK_PID=$!
sleep 1
if ! kill -0 "$MOCK_PID" 2>/dev/null; then
  echo "mock server failed to start"
  cat /tmp/versions-mock-server.log
  exit 1
fi

SIGNAL_DIR="$HOME/.cache/gestalt-versions-demo"
CLIP=/tmp/versions-pr-demo.mov
SCAP="$(command -v scap || true)"
rm -f "$CLIP" "$SIGNAL_DIR/READY" "$SIGNAL_DIR/GO"
: > /tmp/versions-demo-driver.log

npx playwright test e2e/record-versions-demo.spec.ts >> /tmp/versions-demo-driver.log 2>&1 &
DRIVER_PID=$!

for i in $(seq 1 180); do
  test -f "$SIGNAL_DIR/READY" && break
  sleep 0.5
done
test -f "$SIGNAL_DIR/READY" || { echo "driver never ready"; cat /tmp/versions-demo-driver.log; kill "$MOCK_PID" 2>/dev/null || true; exit 1; }

"$SCAP" --app "Google Chrome for Testing" --duration 20000 --fps 60 --force --output "$CLIP" &
SCAP_PID=$!
sleep 0.3
touch "$SIGNAL_DIR/GO"
wait "$SCAP_PID"
wait "$DRIVER_PID" || { cat /tmp/versions-demo-driver.log; kill "$MOCK_PID" 2>/dev/null || true; exit 1; }
kill "$MOCK_PID" 2>/dev/null || true
ls -la "$CLIP"
echo "Recorded clip: $CLIP"
