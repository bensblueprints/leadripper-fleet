# Fleet Worker

Background node client for [fleet.leadripper.com](https://fleet.leadripper.com).

Turns any computer into a silent fleet worker that scrapes Google Maps leads and submits them back to the fleet master.

## Quick Start

### macOS

1. Download `FleetWorker-macOS.zip`
2. Extract it to get `Fleet Worker.app`
3. Double-click `Fleet Worker.app`
4. Enter your license key when prompted
5. The worker installs silently and runs as a background LaunchAgent

To build a DMG on macOS:
```bash
unzip FleetWorker-macOS.zip
bash build-dmg.sh
```

### Windows

1. Download `FleetWorker-Windows.zip`
2. Extract it
3. Run `install.bat YOUR_LICENSE_KEY`
4. The worker installs to `%LOCALAPPDATA%\FleetWorker` and runs via Task Scheduler

## Features

- **Silent background operation** — No UI, no dock icon, runs as a system service
- **Auto-start on login** — Installs as LaunchAgent (macOS) or Scheduled Task (Windows)
- **Resource-aware** — Respects CPU/RAM caps set by fleet admin
- **Built-in scraper** — Uses Playwright to scrape Google Maps directly
- **Heartbeat + job pull** — Connects to fleet API for distributed job execution
- **Command handling** — Responds to pause, resume, kill, and update_caps commands

## Architecture

```
Fleet Worker (Node.js + Bun)
├── Heartbeat → /api/fleet/heartbeat
├── Pull Job  → /api/fleet/pull-job
├── Scrape    → Google Maps via Playwright
└── Submit    → /api/fleet/job-result
```

## Development

```bash
# Install dependencies
bun install

# Run locally
FLEET_LICENSE_KEY=your-key bun run src/worker.js

# Build binaries
bun run build          # macOS ARM64
bun run build:x64      # macOS x64

# Package everything
node scripts/build-all.js
```

## Configuration

The worker reads `~/.fleet-worker/config.json` (macOS) or `%USERPROFILE%\.fleet-worker\config.json` (Windows):

```json
{
  "license_key": "your-license-key",
  "machine_id": "auto-generated",
  "label": "My Mac Mini",
  "cpu_cap": 80,
  "ram_cap": 80
}
```

Environment variables:
- `FLEET_LICENSE_KEY` — overrides config license key
- `FLEET_URL` — defaults to `https://fleet.leadripper.com`

## Uninstall

### macOS
```bash
launchctl unload ~/Library/LaunchAgents/com.leadripper.fleet-worker.plist
rm -rf ~/.fleet-worker
rm ~/Library/LaunchAgents/com.leadripper.fleet-worker.plist
```

### Windows
```cmd
schtasks /Delete /TN FleetWorker /F
rmdir /S /Q %LOCALAPPDATA%\FleetWorker
rmdir /S /Q %USERPROFILE%\.fleet-worker
```
