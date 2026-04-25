#!/usr/bin/env node
/**
 * LeadRipper Fleet Worker
 * Background node that connects to fleet.leadripper.com and executes scrape jobs.
 */

const os = require('os');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { scrapeGoogleMaps, closeBrowser } = require('./scraper');

// ==================== Config ====================
const CONFIG_DIR = path.join(os.homedir(), '.fleet-worker');
const CONFIG_FILE = path.join(CONFIG_DIR, 'config.json');
const LOG_FILE = path.join(CONFIG_DIR, 'worker.log');
const PID_FILE = path.join(CONFIG_DIR, 'worker.pid');

const FLEET_URL = process.env.FLEET_URL || 'https://fleet.leadripper.com';
const HEARTBEAT_INTERVAL_MS = 30_000;
const JOB_POLL_INTERVAL_MS = 5_000;
const OFFLINE_THRESHOLD_MS = 120_000;

// Ensure config dir exists
if (!fs.existsSync(CONFIG_DIR)) fs.mkdirSync(CONFIG_DIR, { recursive: true });

function loadConfig() {
  try {
    return JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
  } catch {
    return {};
  }
}

function saveConfig(cfg) {
  fs.writeFileSync(CONFIG_FILE, JSON.stringify(cfg, null, 2));
}

function generateMachineId() {
  const mac = os.networkInterfaces();
  let seed = '';
  for (const iface of Object.values(mac)) {
    for (const entry of iface || []) {
      if (!entry.internal && entry.mac) {
        seed = entry.mac;
        break;
      }
    }
    if (seed) break;
  }
  seed += os.hostname() + os.userInfo().username;
  return crypto.createHash('sha256').update(seed).digest('hex').slice(0, 16);
}

const config = loadConfig();
if (!config.machine_id) {
  config.machine_id = generateMachineId();
  saveConfig(config);
}
if (!config.license_key) {
  config.license_key = process.env.FLEET_LICENSE_KEY || '';
  if (config.license_key) saveConfig(config);
}
if (config.node_mode == null) config.node_mode = false;
if (config.accepted_terms == null) config.accepted_terms = false;
if (config.cpu_cap == null) config.cpu_cap = config.node_mode ? 50 : 80;
if (config.ram_cap == null) config.ram_cap = config.node_mode ? 50 : 80;
if (config.label == null) config.label = os.hostname();

const TERMS_TEXT = `LeadRipper Fleet Worker — Terms & Conditions

By installing and running Fleet Worker without a license key, you agree that:
1. This device becomes a distributed processing node for the LeadRipper fleet.
2. Up to 50% of available CPU and RAM may be used to process scraping jobs.
3. Jobs run silently in the background with no user interface.
4. You may stop the service at any time via your system settings.

To accept these terms, set accepted_terms=true in your config or use your installer.`;

// ==================== Logging ====================
const logBuffer = [];
const MAX_LOG_BUFFER = 100;

function log(level, msg) {
  const line = `[${new Date().toISOString()}] [${level.toUpperCase()}] ${msg}`;
  console.log(line);
  try {
    fs.appendFileSync(LOG_FILE, line + '\n');
  } catch {}
  logBuffer.push({ level, msg, t: Math.floor(Date.now() / 1000) });
  if (logBuffer.length > MAX_LOG_BUFFER) logBuffer.shift();
}

// ==================== State ====================
let currentJob = null;
let paused = false;
let killed = false;
let cpuCap = config.cpu_cap;
let ramCap = config.ram_cap;

function getSystemStats() {
  const cpus = os.cpus();
  const loadAvg = os.loadavg()[0];
  const cpuPct = cpus.length > 0 ? Math.min(100, Math.round((loadAvg / cpus.length) * 100)) : 0;
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const ramPct = totalMem > 0 ? Math.min(100, Math.round(((totalMem - freeMem) / totalMem) * 100)) : 0;
  return { cpuPct, ramPct };
}

// ==================== Fleet API ====================
async function fleetPost(endpoint, body = {}) {
  const url = `${FLEET_URL}${endpoint}`;
  const headers = {
    'Content-Type': 'application/json',
    'x-license-key': config.license_key,
    'x-machine-id': config.machine_id,
  };
  if (config.node_mode) headers['x-node-mode'] = '1';
  const res = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json();
}

async function fleetGet(endpoint) {
  const url = `${FLEET_URL}${endpoint}`;
  const headers = {
    'x-license-key': config.license_key,
    'x-machine-id': config.machine_id,
  };
  if (config.node_mode) headers['x-node-mode'] = '1';
  const res = await fetch(url, { headers });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json();
}

// ==================== Heartbeat ====================
async function heartbeat() {
  const { cpuPct, ramPct } = getSystemStats();
  const body = {
    hostname: os.hostname(),
    os: `${os.type()} ${os.release()}`,
    app_version: require('../package.json').version,
    cpu_pct: cpuPct,
    ram_pct: ramPct,
    cpu_cap: cpuCap,
    ram_cap: ramCap,
    label: config.label,
    current_job_leads: currentJob?.leadsFound || 0,
    current_job_industry: currentJob?.industry || null,
    current_job_city: currentJob?.city || null,
  };

  // Include buffered logs
  if (logBuffer.length > 0) {
    body.log_lines = logBuffer.splice(0, logBuffer.length);
  }

  const data = await fleetPost('/api/fleet/heartbeat', body);

  if (data.paused !== undefined) paused = data.paused;
  if (data.cpu_cap != null) cpuCap = data.cpu_cap;
  if (data.ram_cap != null) ramCap = data.ram_cap;

  // Process commands
  if (data.commands && data.commands.length > 0) {
    for (const cmd of data.commands) {
      handleCommand(cmd);
    }
  }

  return data;
}

function handleCommand(cmd) {
  log('info', `Command received: ${cmd.kind}`);
  switch (cmd.kind) {
    case 'pause':
      paused = true;
      break;
    case 'resume':
      paused = false;
      break;
    case 'kill':
      killed = true;
      break;
    case 'update_caps':
      if (cmd.payload?.cpu_cap != null) cpuCap = cmd.payload.cpu_cap;
      if (cmd.payload?.ram_cap != null) ramCap = cmd.payload.ram_cap;
      break;
    case 'cancel_job':
      if (currentJob && currentJob.id === cmd.payload?.job_id) {
        currentJob.cancelled = true;
      }
      break;
  }
}

// ==================== Job Execution ====================
async function executeJob(job) {
  currentJob = {
    id: job.id,
    industry: job.industry,
    city: job.city,
    state: job.state,
    max_results: job.max_results || 20,
    leadsFound: 0,
    cancelled: false,
  };

  const query = `${job.industry} in ${job.city}${job.state ? ', ' + job.state : ''}`;
  log('info', `Starting job ${job.id}: ${query} (max ${currentJob.max_results})`);

  let leads = [];
  let error = null;

  try {
    leads = await scrapeGoogleMaps(query, currentJob.max_results);
    currentJob.leadsFound = leads.length;
    log('info', `Job ${job.id} scraped ${leads.length} leads`);
  } catch (err) {
    error = err.message;
    log('error', `Job ${job.id} scrape error: ${error}`);
  }

  // If cancelled mid-scrape, still submit what we have but mark as cancelled
  if (currentJob.cancelled) {
    log('info', `Job ${job.id} was cancelled`);
  }

  // Submit results
  try {
    const mappedLeads = leads.map(l => ({
      name: l.business_name || l.name || '',
      phone: l.phone || null,
      email: l.email || null,
      website: l.website || null,
      address: l.address || null,
      city: l.city || job.city,
      state: l.state || job.state || null,
      industry: l.industry || job.industry || null,
      gcid: l.place_id || l.gcid || null,
      search_term: query,
      google_category_raw: l.google_category_raw || null,
      rating: l.rating || null,
      reviews: l.reviews || null,
      business_hours: l.business_hours || l.hours || null,
    }));

    await fleetPost('/api/fleet/job-result', {
      job_id: job.id,
      leads: mappedLeads,
      error: currentJob.cancelled ? 'cancelled' : error,
    });
    log('info', `Job ${job.id} results submitted`);
  } catch (err) {
    log('error', `Job ${job.id} result submission failed: ${err.message}`);
  }

  currentJob = null;
}

async function pullAndExecuteJob() {
  if (paused) return;
  if (currentJob) return;

  const { cpuPct, ramPct } = getSystemStats();
  if (cpuPct > cpuCap || ramPct > ramCap) {
    log('info', `Resource limits exceeded: CPU ${cpuPct}% (cap ${cpuCap}%) RAM ${ramPct}% (cap ${ramCap}%)`);
    return;
  }

  try {
    const data = await fleetPost('/api/fleet/pull-job');
    if (!data.job) return;

    await executeJob(data.job);
  } catch (err) {
    log('error', `Pull job error: ${err.message}`);
  }
}

// ==================== Main Loop ====================
async function main() {
  if (!config.license_key && !config.node_mode) {
    log('info', 'No license key found. Auto-enabling NODE MODE.');
    log('info', TERMS_TEXT);
    config.node_mode = true;
    config.accepted_terms = true;
    config.cpu_cap = 50;
    config.ram_cap = 50;
    saveConfig(config);
  }

  if (config.node_mode) {
    cpuCap = 50;
    ramCap = 50;
    log('info', 'Running in NODE MODE — up to 50% CPU/RAM will be used for fleet jobs');
  }

  // Write PID file
  fs.writeFileSync(PID_FILE, String(process.pid));

  log('info', `Fleet Worker starting — machine_id=${config.machine_id} fleet=${FLEET_URL} mode=${config.node_mode ? 'node' : 'licensed'}`);

  // Initial heartbeat
  try {
    await heartbeat();
    log('info', 'Registered with fleet server');
  } catch (err) {
    log('error', `Initial heartbeat failed: ${err.message}`);
  }

  // Heartbeat loop
  const hbInterval = setInterval(async () => {
    if (killed) {
      clearInterval(hbInterval);
      clearInterval(jobInterval);
      log('info', 'Killed by server command');
      await closeBrowser();
      process.exit(0);
    }
    try {
      await heartbeat();
    } catch (err) {
      log('error', `Heartbeat error: ${err.message}`);
    }
  }, HEARTBEAT_INTERVAL_MS);

  // Job polling loop
  const jobInterval = setInterval(async () => {
    try {
      await pullAndExecuteJob();
    } catch (err) {
      log('error', `Job loop error: ${err.message}`);
    }
  }, JOB_POLL_INTERVAL_MS);

  // Also poll immediately
  pullAndExecuteJob().catch(() => {});
}

// ==================== Signal Handlers ====================
process.on('SIGINT', async () => {
  log('info', 'SIGINT received, shutting down...');
  await closeBrowser();
  try { fs.unlinkSync(PID_FILE); } catch {}
  process.exit(0);
});

process.on('SIGTERM', async () => {
  log('info', 'SIGTERM received, shutting down...');
  await closeBrowser();
  try { fs.unlinkSync(PID_FILE); } catch {}
  process.exit(0);
});

process.on('uncaughtException', (err) => {
  log('error', `Uncaught exception: ${err.message}`);
});

process.on('unhandledRejection', (err) => {
  log('error', `Unhandled rejection: ${err?.message || err}`);
});

main().catch((err) => {
  log('error', `Fatal: ${err.message}`);
  process.exit(1);
});
