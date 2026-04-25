#!/bin/bash
# Fleet Worker Launcher
# Installs and starts the fleet worker background service on first run.

set -e

APP_NAME="Fleet Worker"
BUNDLE_ID="com.leadripper.fleet-worker"
WORKER_DIR="$HOME/.fleet-worker"
BIN_DIR="$WORKER_DIR/bin"
CONFIG_FILE="$WORKER_DIR/config.json"
PLIST_SRC="$(dirname "$0")/../Resources/com.leadripper.fleet-worker.plist"
PLIST_DST="$HOME/Library/LaunchAgents/$BUNDLE_ID.plist"
RESOURCES_DIR="$(dirname "$0")/../Resources"

# Detect architecture
ARCH=$(uname -m)
if [ "$ARCH" = "arm64" ]; then
    BINARY_SRC="$RESOURCES_DIR/fleet-worker"
elif [ "$ARCH" = "x86_64" ]; then
    BINARY_SRC="$RESOURCES_DIR/fleet-worker-x64"
    if [ ! -f "$BINARY_SRC" ]; then
        BINARY_SRC="$RESOURCES_DIR/fleet-worker"
    fi
else
    BINARY_SRC="$RESOURCES_DIR/fleet-worker"
fi

BINARY_DST="$BIN_DIR/fleet-worker"

# Create directories
mkdir -p "$BIN_DIR"
mkdir -p "$WORKER_DIR"

# Copy binary if newer
if [ ! -f "$BINARY_DST" ] || [ "$BINARY_SRC" -nt "$BINARY_DST" ]; then
    cp -f "$BINARY_SRC" "$BINARY_DST"
    chmod +x "$BINARY_DST"
fi

# Copy node_modules for runtime
if [ -d "$RESOURCES_DIR/node_modules" ]; then
    rm -rf "$WORKER_DIR/node_modules"
    cp -R "$RESOURCES_DIR/node_modules" "$WORKER_DIR/node_modules"
fi

# Prompt for license key if not configured
if [ ! -f "$CONFIG_FILE" ] || ! grep -q '"license_key"' "$CONFIG_FILE" 2>/dev/null; then
    LICENSE_KEY=$(osascript -e 'text returned of (display dialog "Enter your LeadRipper Fleet license key:" default answer "" with title "Fleet Worker Setup")' 2>/dev/null || true)
    if [ -n "$LICENSE_KEY" ]; then
        echo "{ \"license_key\": \"$LICENSE_KEY\" }" > "$CONFIG_FILE"
    else
        osascript -e 'display alert "Fleet Worker" message "A license key is required. Please run Fleet Worker again and enter your license key."' 2>/dev/null || true
        exit 1
    fi
fi

# Install launch agent plist
if [ -f "$PLIST_SRC" ]; then
    sed "s|HOME_DIR|$HOME|g" "$PLIST_SRC" > "$PLIST_DST"
    chmod 644 "$PLIST_DST"
fi

# Load / reload service
if launchctl list "$BUNDLE_ID" &>/dev/null; then
    launchctl unload "$PLIST_DST" 2>/dev/null || true
fi
launchctl load "$PLIST_DST" 2>/dev/null || launchctl bootstrap gui/$(id -u) "$PLIST_DST" 2>/dev/null || true

# Show notification
osascript -e "display notification \"$APP_NAME is now running in the background.\" with title \"$APP_NAME\"" 2>/dev/null || true

exit 0
