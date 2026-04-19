const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

const DATA_DIR = process.env.DATA_DIR || '/data';
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const db = new Database(path.join(DATA_DIR, 'fleet.db'));
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    machine_id TEXT UNIQUE NOT NULL,
    license_key TEXT NOT NULL,
    hostname TEXT,
    label TEXT,
    os TEXT,
    app_version TEXT,
    cpu_pct REAL DEFAULT 0,
    ram_pct REAL DEFAULT 0,
    cpu_cap REAL DEFAULT 50,
    ram_cap REAL DEFAULT 50,
    status TEXT DEFAULT 'offline',
    current_job_id INTEGER,
    last_seen INTEGER,
    first_seen INTEGER,
    jobs_done INTEGER DEFAULT 0,
    leads_harvested INTEGER DEFAULT 0,
    paused INTEGER DEFAULT 0
  );

  CREATE INDEX IF NOT EXISTS idx_nodes_license ON nodes(license_key);
  CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);

  CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    industry TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT,
    target_node_id INTEGER,
    assigned_node_id INTEGER,
    status TEXT DEFAULT 'queued',
    priority INTEGER DEFAULT 0,
    leads_found INTEGER DEFAULT 0,
    error TEXT,
    created_at INTEGER DEFAULT (strftime('%s','now')),
    assigned_at INTEGER,
    started_at INTEGER,
    finished_at INTEGER
  );

  CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
  CREATE INDEX IF NOT EXISTS idx_jobs_assigned ON jobs(assigned_node_id);

  CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    node_id INTEGER,
    name TEXT,
    phone TEXT,
    email TEXT,
    website TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    industry TEXT,
    gcid TEXT,
    search_term TEXT,
    google_category_raw TEXT,
    rating REAL,
    reviews INTEGER,
    created_at INTEGER DEFAULT (strftime('%s','now')),
    synced_at INTEGER
  );

  CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads(phone);
  CREATE INDEX IF NOT EXISTS idx_leads_job ON leads(job_id);
  CREATE INDEX IF NOT EXISTS idx_leads_synced ON leads(synced_at);

  CREATE TABLE IF NOT EXISTS commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT,
    delivered INTEGER DEFAULT 0,
    created_at INTEGER DEFAULT (strftime('%s','now'))
  );
  CREATE INDEX IF NOT EXISTS idx_commands_node ON commands(node_id, delivered);
`);

// Sweep stale nodes on startup — any node not seen in 90s is offline
const stmtMarkOffline = db.prepare(`
  UPDATE nodes SET status='offline' WHERE last_seen < ? AND status != 'offline'
`);
setInterval(() => {
  try { stmtMarkOffline.run(Math.floor(Date.now() / 1000) - 90); } catch (e) {}
}, 15000);

module.exports = db;
