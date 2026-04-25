#!/bin/bash
APP="/Volumes/Fleet Worker/Fleet Worker.app"
if [ -d "$APP" ]; then
  xattr -d com.apple.quarantine "$APP" 2>/dev/null || true
  bash "$APP/Contents/MacOS/fleet-worker-launcher"
fi
