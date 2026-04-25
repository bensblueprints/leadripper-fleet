#!/bin/bash
set -e
BUILD_DIR="$HOME/fleet-worker-build"
APP_BUNDLE="$BUILD_DIR/Fleet Worker.app"
DMG_NAME="$BUILD_DIR/FleetWorker.dmg"
VOLUME_NAME="Fleet Worker"

# Rebuild app with new launcher
rm -rf "$APP_BUNDLE"
mkdir -p "$APP_BUNDLE/Contents/MacOS"
mkdir -p "$APP_BUNDLE/Contents/Resources"

cp "$BUILD_DIR/assets/Info.plist" "$APP_BUNDLE/Contents/Info.plist"
cp "$BUILD_DIR/assets/launcher.sh" "$APP_BUNDLE/Contents/MacOS/fleet-worker-launcher"
chmod +x "$APP_BUNDLE/Contents/MacOS/fleet-worker-launcher"

cp "$BUILD_DIR/fleet-worker-mac" "$APP_BUNDLE/Contents/Resources/fleet-worker"
chmod +x "$APP_BUNDLE/Contents/Resources/fleet-worker"
cp "$BUILD_DIR/fleet-worker-mac-x64" "$APP_BUNDLE/Contents/Resources/fleet-worker-x64"
chmod +x "$APP_BUNDLE/Contents/Resources/fleet-worker-x64"

cp -R "$BUILD_DIR/node_modules" "$APP_BUNDLE/Contents/Resources/node_modules"
cp "$BUILD_DIR/assets/com.leadripper.fleet-worker.plist" "$APP_BUNDLE/Contents/Resources/com.leadripper.fleet-worker.plist"

# Create DMG
rm -f "$DMG_NAME"
TMP_DIR=$(mktemp -d)
cp -R "$APP_BUNDLE" "$TMP_DIR/"
ln -s /Applications "$TMP_DIR/Applications"
hdiutil create -volname "$VOLUME_NAME" -srcfolder "$TMP_DIR" -ov -format UDZO "$DMG_NAME"
rm -rf "$TMP_DIR"
echo "DMG rebuilt: $DMG_NAME"
