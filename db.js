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
    max_results INTEGER DEFAULT 20,
    leads_found INTEGER DEFAULT 0,
    error TEXT,
    created_at INTEGER DEFAULT (strftime('%s','now')),
    assigned_at INTEGER,
    started_at INTEGER,
    finished_at INTEGER
  );

  CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
  CREATE INDEX IF NOT EXISTS idx_jobs_assigned ON jobs(assigned_node_id);
`);

// Migration: add max_results if missing (for DBs created before v1.1)
try {
  const cols = db.prepare(`PRAGMA table_info(jobs)`).all().map(c => c.name);
  if (!cols.includes('max_results')) {
    db.exec(`ALTER TABLE jobs ADD COLUMN max_results INTEGER DEFAULT 20`);
  }
} catch {}

// Migration: enrichment + new metrics + AI analysis columns on leads
try {
  const lcols = db.prepare(`PRAGMA table_info(leads)`).all().map(c => c.name);
  if (!lcols.includes('website_platform')) db.exec(`ALTER TABLE leads ADD COLUMN website_platform TEXT`);
  if (!lcols.includes('website_status')) db.exec(`ALTER TABLE leads ADD COLUMN website_status TEXT DEFAULT 'unchecked'`);
  if (!lcols.includes('tags')) db.exec(`ALTER TABLE leads ADD COLUMN tags TEXT DEFAULT '[]'`);
  if (!lcols.includes('business_hours')) db.exec(`ALTER TABLE leads ADD COLUMN business_hours TEXT`);
  if (!lcols.includes('reviews_1star')) db.exec(`ALTER TABLE leads ADD COLUMN reviews_1star INTEGER DEFAULT 0`);
  if (!lcols.includes('reviews_2star')) db.exec(`ALTER TABLE leads ADD COLUMN reviews_2star INTEGER DEFAULT 0`);
  if (!lcols.includes('reviews_3star')) db.exec(`ALTER TABLE leads ADD COLUMN reviews_3star INTEGER DEFAULT 0`);
  if (!lcols.includes('reviews_4star')) db.exec(`ALTER TABLE leads ADD COLUMN reviews_4star INTEGER DEFAULT 0`);
  if (!lcols.includes('reviews_5star')) db.exec(`ALTER TABLE leads ADD COLUMN reviews_5star INTEGER DEFAULT 0`);
  if (!lcols.includes('ai_seo_score')) db.exec(`ALTER TABLE leads ADD COLUMN ai_seo_score INTEGER`);
  if (!lcols.includes('ai_design_score')) db.exec(`ALTER TABLE leads ADD COLUMN ai_design_score INTEGER`);
  if (!lcols.includes('ai_seo_notes')) db.exec(`ALTER TABLE leads ADD COLUMN ai_seo_notes TEXT`);
  if (!lcols.includes('ai_design_notes')) db.exec(`ALTER TABLE leads ADD COLUMN ai_design_notes TEXT`);
  if (!lcols.includes('ai_analyzed_at')) db.exec(`ALTER TABLE leads ADD COLUMN ai_analyzed_at INTEGER`);
  if (!lcols.includes('ai_provider')) db.exec(`ALTER TABLE leads ADD COLUMN ai_provider TEXT`);
} catch {}

// Partial unique index for dedupe on sync-leads (tolerates legacy dupes; new inserts use OR IGNORE)
try {
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS uniq_leads_phone_industry ON leads(phone, industry) WHERE phone IS NOT NULL AND industry IS NOT NULL`);
} catch (e) { console.warn('[fleet] dedup index skipped:', e.message); }

db.exec(`

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
    website_platform TEXT,
    website_status TEXT DEFAULT 'unchecked',
    tags TEXT DEFAULT '[]',
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

  CREATE TABLE IF NOT EXISTS node_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id INTEGER NOT NULL,
    job_id INTEGER,
    level TEXT DEFAULT 'info',
    msg TEXT,
    t INTEGER DEFAULT (strftime('%s','now'))
  );
  CREATE INDEX IF NOT EXISTS idx_node_logs_node_id ON node_logs(node_id, id DESC);
`);

// Keep last 500 log lines per node
setInterval(() => {
  try {
    db.exec(`
      DELETE FROM node_logs WHERE id IN (
        SELECT id FROM (
          SELECT id, ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY id DESC) AS rn
          FROM node_logs
        ) WHERE rn > 500
      )
    `);
  } catch {}
}, 60000);

// Sweep stale nodes on startup — any node not seen in 90s is offline
const stmtMarkOffline = db.prepare(`
  UPDATE nodes SET status='offline' WHERE last_seen < ? AND status != 'offline'
`);
setInterval(() => {
  try { stmtMarkOffline.run(Math.floor(Date.now() / 1000) - 90); } catch (e) {}
}, 15000);

module.exports = db;
