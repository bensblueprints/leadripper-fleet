#!/bin/bash
set -e
BUILD_DIR="$HOME/fleet-worker-build"
APP_BUNDLE="Fleet Worker.app"
DMG_NAME="FleetWorker.dmg"
VOLUME_NAME="Fleet Worker"

cd "$BUILD_DIR"
rm -rf "$APP_BUNDLE"
rm -f "$DMG_NAME"

mkdir -p "$APP_BUNDLE/Contents/MacOS"
mkdir -p "$APP_BUNDLE/Contents/Resources"

# Info.plist
cp "assets/Info.plist" "$APP_BUNDLE/Contents/Info.plist"

# Launcher script
cp "assets/launcher.sh" "$APP_BUNDLE/Contents/MacOS/fleet-worker-launcher"
chmod +x "$APP_BUNDLE/Contents/MacOS/fleet-worker-launcher"

# Binaries
cp "fleet-worker-mac" "$APP_BUNDLE/Contents/Resources/fleet-worker"
chmod +x "$APP_BUNDLE/Contents/Resources/fleet-worker"
cp "fleet-worker-mac-x64" "$APP_BUNDLE/Contents/Resources/fleet-worker-x64"
chmod +x "$APP_BUNDLE/Contents/Resources/fleet-worker-x64"

# node_modules
cp -R "node_modules" "$APP_BUNDLE/Contents/Resources/node_modules"

# Plist template
cp "assets/com.leadripper.fleet-worker.plist" "$APP_BUNDLE/Contents/Resources/com.leadripper.fleet-worker.plist"

# Create DMG
TMP_DIR=$(mktemp -d)
cp -R "$APP_BUNDLE" "$TMP_DIR/"
ln -s /Applications "$TMP_DIR/Applications"

hdiutil create -volname "$VOLUME_NAME" -srcfolder "$TMP_DIR" -ov -format UDZO "$DMG_NAME"

echo "DMG created: $DMG_NAME"
rm -rf "$TMP_DIR"
