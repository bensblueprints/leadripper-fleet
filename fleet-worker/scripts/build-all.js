#!/usr/bin/env node
/**
 * Build script for Fleet Worker
 * Produces:
 *   - dist/FleetWorker-Windows.zip  (Windows EXE + node_modules + installer)
 *   - dist/FleetWorker-macOS.zip    (macOS .app bundle)
 *   - dist/build-dmg.sh             (Script to build DMG on macOS)
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const ROOT = path.join(__dirname, '..');
const BUILD_DIR = path.join(ROOT, 'build');
const DIST_DIR = path.join(ROOT, 'dist');
const SRC_DIR = path.join(ROOT, 'src');

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function cleanDir(dir) {
  if (fs.existsSync(dir)) {
    fs.rmSync(dir, { recursive: true });
  }
  fs.mkdirSync(dir, { recursive: true });
}

function copyDir(src, dest) {
  ensureDir(dest);
  const entries = fs.readdirSync(src, { withFileTypes: true });
  for (const entry of entries) {
    const srcPath = path.join(src, entry.name);
    const destPath = path.join(dest, entry.name);
    if (entry.isDirectory()) {
      copyDir(srcPath, destPath);
    } else {
      fs.copyFileSync(srcPath, destPath);
    }
  }
}

function run(cmd, opts = {}) {
  console.log(`> ${cmd}`);
  return execSync(cmd, { cwd: ROOT, stdio: 'inherit', ...opts });
}

// ==================== Build Binaries ====================
console.log('\n=== Building Fleet Worker binaries ===\n');

try {
  run('bun build --compile --target=bun-windows-x64 --external=playwright-core src/worker.js --outfile build/fleet-worker.exe');
} catch (e) {
  console.error('Windows build failed. Trying with node...');
  process.exit(1);
}

try {
  run('bun build --compile --target=bun-darwin-arm64 --external=playwright-core src/worker.js --outfile build/fleet-worker-mac');
} catch (e) {
  console.error('macOS ARM build failed.');
}

try {
  run('bun build --compile --target=bun-darwin-x64 --external=playwright-core src/worker.js --outfile build/fleet-worker-mac-x64');
} catch (e) {
  console.error('macOS x64 build failed.');
}

// ==================== Windows Package ====================
console.log('\n=== Packaging Windows ===\n');

const winDir = path.join(DIST_DIR, 'FleetWorker-Windows');
cleanDir(winDir);

// Copy binary (renamed for cleaner UX)
fs.copyFileSync(path.join(BUILD_DIR, 'fleet-worker.exe'), path.join(winDir, 'FleetWorker.exe'));

// Copy node_modules for runtime (playwright-core is external)
const nodeModsSrc = path.join(ROOT, 'node_modules');
const nodeModsDest = path.join(winDir, 'node_modules');
if (fs.existsSync(nodeModsSrc)) {
  copyDir(nodeModsSrc, nodeModsDest);
}

// Create README
fs.writeFileSync(path.join(winDir, 'README.txt'), `Fleet Worker for Windows
==========================

1. Extract this zip anywhere
2. Double-click FleetWorker.exe to run
3. It connects to the fleet and starts processing jobs immediately
4. Logs and config are at %%USERPROFILE%%\\.fleet-worker\\worker.log

To add a license key:
  Edit %%USERPROFILE%%\\.fleet-worker\\config.json and add:
  { "license_key": "your-key-here" }

To stop:
  Close the FleetWorker.exe window or press Ctrl+C
`);

// Zip it
const winZip = path.join(DIST_DIR, 'FleetWorker-Windows.zip');
if (fs.existsSync(winZip)) fs.unlinkSync(winZip);
try {
  run(`powershell -Command "Compress-Archive -Path '${winDir}\*' -DestinationPath '${winZip}'"`);
} catch (e) {
  console.error('Failed to zip Windows package');
}

// ==================== macOS App Bundle ====================
console.log('\n=== Packaging macOS ===\n');

const appBundleName = 'Fleet Worker.app';
const appBundleDir = path.join(DIST_DIR, appBundleName);
cleanDir(appBundleDir);

const contentsDir = path.join(appBundleDir, 'Contents');
const macOSDir = path.join(contentsDir, 'MacOS');
const resourcesDir = path.join(contentsDir, 'Resources');

ensureDir(macOSDir);
ensureDir(resourcesDir);

// Info.plist
fs.copyFileSync(path.join(ROOT, 'assets', 'Info.plist'), path.join(contentsDir, 'Info.plist'));

// Launcher script
const launcherSrc = fs.readFileSync(path.join(ROOT, 'assets', 'launcher.sh'), 'utf8');
const launcherPath = path.join(macOSDir, 'fleet-worker-launcher');
fs.writeFileSync(launcherPath, launcherSrc);

// Binary (use universal binary if possible, otherwise arm64)
const macBinary = fs.existsSync(path.join(BUILD_DIR, 'fleet-worker-mac'))
  ? path.join(BUILD_DIR, 'fleet-worker-mac')
  : path.join(BUILD_DIR, 'fleet-worker-mac-x64');
if (fs.existsSync(macBinary)) {
  fs.copyFileSync(macBinary, path.join(resourcesDir, 'fleet-worker'));
}

// Also copy x64 as fallback
if (fs.existsSync(path.join(BUILD_DIR, 'fleet-worker-mac-x64'))) {
  fs.copyFileSync(path.join(BUILD_DIR, 'fleet-worker-mac-x64'), path.join(resourcesDir, 'fleet-worker-x64'));
}

// node_modules for runtime
copyDir(nodeModsSrc, path.join(resourcesDir, 'node_modules'));

// Plist template
fs.copyFileSync(path.join(ROOT, 'assets', 'com.leadripper.fleet-worker.plist'), path.join(resourcesDir, 'com.leadripper.fleet-worker.plist'));

// README inside Resources
fs.writeFileSync(path.join(resourcesDir, 'README.txt'), `Fleet Worker for macOS
========================

1. Double-click "Fleet Worker.app" to install and start
2. The worker installs to ~/.fleet-worker/
3. It runs as a LaunchAgent (auto-starts on login)
4. Add your license key to ~/.fleet-worker/config.json

To uninstall:
  launchctl unload ~/Library/LaunchAgents/com.leadripper.fleet-worker.plist
  rm -rf ~/.fleet-worker
  rm ~/Library/LaunchAgents/com.leadripper.fleet-worker.plist
`);

// Zip the app bundle
const macZip = path.join(DIST_DIR, 'FleetWorker-macOS.zip');
if (fs.existsSync(macZip)) fs.unlinkSync(macZip);
try {
  run(`powershell -Command "Compress-Archive -Path '${appBundleDir}' -DestinationPath '${macZip}'"`);
} catch (e) {
  console.error('Failed to zip macOS package');
}

// ==================== DMG Build Script ====================
console.log('\n=== Creating DMG build script ===\n');

const dmgScript = `#!/bin/bash
# Build DMG on macOS
# Run this script on a Mac after extracting FleetWorker-macOS.zip

set -e

APP_NAME="Fleet Worker"
APP_BUNDLE="Fleet Worker.app"
DMG_NAME="FleetWorker.dmg"
VOLUME_NAME="Fleet Worker"
TMP_DIR=$(mktemp -d)

echo "Building DMG..."

# Copy app bundle to temp dir
cp -R "\${APP_BUNDLE}" "\${TMP_DIR}/"

# Create a symlink to Applications
ln -s /Applications "\${TMP_DIR}/Applications"

# Create DMG
hdiutil create -volname "\${VOLUME_NAME}" -srcfolder "\${TMP_DIR}" -ov -format UDZO "\${DMG_NAME}"

echo "DMG created: \${DMG_NAME}"
rm -rf "\${TMP_DIR}"
`;

fs.writeFileSync(path.join(DIST_DIR, 'build-dmg.sh'), dmgScript);

// ==================== Summary ====================
console.log('\n========================================');
console.log('Build complete!');
console.log('========================================');
console.log('');
console.log('Outputs:');
console.log('  dist/FleetWorker-Windows.zip   - Windows installer package');
console.log('  dist/FleetWorker-macOS.zip     - macOS app bundle (zip)');
console.log('  dist/build-dmg.sh              - Run on Mac to create DMG');
console.log('  dist/Fleet Worker.app/         - macOS app bundle (folder)');
console.log('');
