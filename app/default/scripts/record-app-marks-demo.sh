#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
SIGNAL_DIR="$HOME/.cache/gestalt-app-marks-demo"
CLIP=/tmp/app-marks-demo-clip.mov
SCAP=/tmp/SwiftCapture/.build/release/SwiftCapture
rm -f "$CLIP" "$SIGNAL_DIR/READY" "$SIGNAL_DIR/GO"
: > /tmp/app-marks-demo-driver.log

npx playwright test e2e/record-app-marks-demo.spec.ts >> /tmp/app-marks-demo-driver.log 2>&1 &
DRIVER_PID=$!

for i in $(seq 1 120); do
  test -f "$SIGNAL_DIR/READY" && break
  sleep 0.5
done
test -f "$SIGNAL_DIR/READY" || { echo "driver never ready"; cat /tmp/app-marks-demo-driver.log; exit 1; }

"$SCAP" --app "Google Chrome for Testing" --duration 18000 --fps 60 --force --output "$CLIP" &
SCAP_PID=$!
sleep 0.3
touch "$SIGNAL_DIR/GO"
wait "$SCAP_PID"
wait "$DRIVER_PID" || { cat /tmp/app-marks-demo-driver.log; exit 1; }
ls -la "$CLIP"
echo DONE
