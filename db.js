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

// Migration: live progress fields on nodes
try {
  const ncols = db.prepare(`PRAGMA table_info(nodes)`).all().map(c => c.name);
  if (!ncols.includes('current_job_leads')) db.exec(`ALTER TABLE nodes ADD COLUMN current_job_leads INTEGER DEFAULT 0`);
  if (!ncols.includes('current_job_industry')) db.exec(`ALTER TABLE nodes ADD COLUMN current_job_industry TEXT`);
  if (!ncols.includes('current_job_city')) db.exec(`ALTER TABLE nodes ADD COLUMN current_job_city TEXT`);
} catch {}

// Migration: recategorize queue column on leads
try {
  const lcols2 = db.prepare(`PRAGMA table_info(leads)`).all().map(c => c.name);
  if (!lcols2.includes('recat_status')) db.exec(`ALTER TABLE leads ADD COLUMN recat_status TEXT`); // NULL | 'queued' | 'done'
  db.exec(`CREATE INDEX IF NOT EXISTS idx_leads_recat ON leads(recat_status)`);
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
  if (!lcols.includes('ghl_synced')) db.exec(`ALTER TABLE leads ADD COLUMN ghl_synced INTEGER DEFAULT 0`);
  if (!lcols.includes('ghl_contact_id')) db.exec(`ALTER TABLE leads ADD COLUMN ghl_contact_id TEXT`);
  if (!lcols.includes('ghl_synced_at')) db.exec(`ALTER TABLE leads ADD COLUMN ghl_synced_at INTEGER`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_leads_ghl_synced ON leads(ghl_synced)`);
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

// ---- Mass-scrape orchestrator tables ----
db.exec(`
  CREATE TABLE IF NOT EXISTS scrape_coverage (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    industry      TEXT NOT NULL,
    gcid          TEXT NOT NULL,
    city          TEXT NOT NULL,
    state         TEXT NOT NULL,
    phase         INTEGER NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    job_id        INTEGER,
    leads_found   INTEGER DEFAULT 0,
    last_error    TEXT,
    dispatched_at INTEGER,
    completed_at  INTEGER,
    UNIQUE(industry, city)
  );
  CREATE INDEX IF NOT EXISTS idx_cov_status_phase ON scrape_coverage(status, phase);
  CREATE INDEX IF NOT EXISTS idx_cov_job ON scrape_coverage(job_id);

  CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT
  );

  CREATE TABLE IF NOT EXISTS mass_scrape_log (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    t      INTEGER NOT NULL,
    level  TEXT DEFAULT 'info',
    action TEXT,
    reason TEXT,
    meta   TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_ms_log_t ON mass_scrape_log(t DESC);
`);

// Seed default settings if missing
const defaultSettings = {
  mass_scrape_enabled: '0',
  mass_scrape_max_inflight_per_worker: '2',
  mass_scrape_industry_filter: 'home-services',
  mass_scrape_tick_sec: '30',
  mass_scrape_ai_monitor_enabled: '0'
};
for (const [k, v] of Object.entries(defaultSettings)) {
  db.prepare(`INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)`).run(k, v);
}

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
// Also free any jobs they were holding so other workers can pick them up.
function sweepStaleNodes() {
  const t = Math.floor(Date.now() / 1000) - 90;
  const tx = db.transaction(() => {
    const stale = db.prepare(`SELECT id, current_job_id FROM nodes WHERE last_seen < ? AND status != 'offline'`).all(t);
    for (const n of stale) {
      if (n.current_job_id) {
        db.prepare(`UPDATE jobs SET status='queued', assigned_node_id=NULL, started_at=NULL WHERE id=? AND status='running'`).run(n.current_job_id);
      }
      db.prepare(`UPDATE nodes SET status='offline', current_job_id=NULL WHERE id=?`).run(n.id);
    }
  });
  try { tx(); } catch (e) {}
}
setInterval(sweepStaleNodes, 15000);
sweepStaleNodes(); // run once on boot

module.exports = db;
