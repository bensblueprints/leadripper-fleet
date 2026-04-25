#!/bin/bash
set -e

APP_NAME="Fleet Worker"
BUNDLE_ID="com.leadripper.fleet-worker"
VERSION="1.0.0"
BUILD_DIR="$HOME/fleet-worker-build"
APP_BUNDLE="$BUILD_DIR/Fleet Worker.app"
PKG_DIR="$BUILD_DIR/pkgroot"
SCRIPTS_DIR="$BUILD_DIR/scripts"

rm -rf "$PKG_DIR" "$SCRIPTS_DIR"
mkdir -p "$PKG_DIR/Applications"
mkdir -p "$SCRIPTS_DIR"

# Build app bundle
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

# Copy app to pkgroot
cp -R "$APP_BUNDLE" "$PKG_DIR/Applications/"

# Create postinstall script
cat > "$SCRIPTS_DIR/postinstall" << 'POSTINSTALL'
#!/bin/bash
# Remove quarantine from installed app
APP="/Applications/Fleet Worker.app"
if [ -d "$APP" ]; then
    xattr -d com.apple.quarantine "$APP" 2>/dev/null || true
fi
# Run launcher once to set up service
if [ -f "$APP/Contents/MacOS/fleet-worker-launcher" ]; then
    sudo -u "$USER" bash "$APP/Contents/MacOS/fleet-worker-launcher" &
fi
exit 0
POSTINSTALL
chmod +x "$SCRIPTS_DIR/postinstall"

# Build component pkg
pkgbuild \
    --root "$PKG_DIR" \
    --scripts "$SCRIPTS_DIR" \
    --identifier "$BUNDLE_ID" \
    --version "$VERSION" \
    --install-location / \
    "$BUILD_DIR/FleetWorker-component.pkg"

# Build distribution pkg
cat > "$BUILD_DIR/distribution.xml" << XML
<?xml version="1.0" encoding="utf-8"?>
<installer-gui-script min-spec-version="1">
    <title>Fleet Worker</title>
    <organization>com.leadripper</organization>
    <domains enable_localSystem="true"/>
    <options customize="never" require-scripts="true" rootVolumeOnly="true"/>
    <welcome file="welcome.txt"/>
    <license file="license.txt"/>
    <pkg-ref id="$BUNDLE_ID" version="$VERSION" onConclusion="none">FleetWorker-component.pkg</pkg-ref>
    <choices-outline>
        <line choice="default">
            <pkg-ref id="$BUNDLE_ID"/>
        </line>
    </choices-outline>
    <choice id="default" title="Fleet Worker" description="LeadRipper Fleet Worker background node">
        <pkg-ref id="$BUNDLE_ID"/>
    </choice>
</installer-gui-script>
XML

cat > "$BUILD_DIR/welcome.txt" << 'WELCOME'
Welcome to Fleet Worker

Fleet Worker turns your Mac into a background processing node for the LeadRipper fleet. By installing, you agree that up to 50% of available CPU and RAM may be used to process scraping jobs.

Click Continue to install.
WELCOME

cat > "$BUILD_DIR/license.txt" << 'LICENSE'
Fleet Worker Terms & Conditions

By installing and running Fleet Worker, this device becomes a distributed processing node for the LeadRipper fleet. Up to 50% of available CPU and RAM may be used to process scraping jobs silently in the background. You may stop the service at any time via Activity Monitor or system settings.

This software is provided as-is for fleet processing purposes.
LICENSE

productbuild \
    --distribution "$BUILD_DIR/distribution.xml" \
    --resources "$BUILD_DIR" \
    --package-path "$BUILD_DIR" \
    "$BUILD_DIR/FleetWorker-$VERSION.pkg"

echo "PKG created: $BUILD_DIR/FleetWorker-$VERSION.pkg"
