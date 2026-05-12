// One-shot: create lookup tables, populate from JSON, add jobs columns, set toggles.
// Run inside container: docker exec leadripper-fleet node /app/bootstrap-dispatcher.js
const Database = require('better-sqlite3');
const fs = require('fs');

const db = new Database('/data/fleet.db');
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

console.log('--- creating tables ---');
db.exec(`
  CREATE TABLE IF NOT EXISTS industries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    gcid TEXT NOT NULL,
    name TEXT NOT NULL UNIQUE
  );
  CREATE TABLE IF NOT EXISTS cities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city_state TEXT NOT NULL UNIQUE,
    city TEXT NOT NULL,
    state TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS jobs_completed (
    industry_id INTEGER NOT NULL,
    city_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    completed_at INTEGER,
    leads_found INTEGER DEFAULT 0,
    node_id INTEGER,
    last_error TEXT,
    PRIMARY KEY (industry_id, city_id)
  );
`);

function hasColumn(table, col) {
  return db.prepare(`PRAGMA table_info(${table})`).all().some(r => r.name === col);
}
if (!hasColumn('jobs', 'industry_id')) db.exec('ALTER TABLE jobs ADD COLUMN industry_id INTEGER');
if (!hasColumn('jobs', 'city_id')) db.exec('ALTER TABLE jobs ADD COLUMN city_id INTEGER');

console.log('--- populating industries ---');
const inds = JSON.parse(fs.readFileSync('/app/mass-scrape/industries-all.json', 'utf8'));
const insInd = db.prepare('INSERT OR IGNORE INTO industries (gcid, name) VALUES (?, ?)');
let indAdded = 0;
db.transaction(() => {
  for (const i of inds) {
    const r = insInd.run(i.gcid, i.name);
    if (r.changes) indAdded++;
  }
})();
const indCount = db.prepare('SELECT COUNT(*) AS c FROM industries').get().c;
console.log(`industries: total=${indCount} new=${indAdded}`);

console.log('--- populating cities ---');
const cs = JSON.parse(fs.readFileSync('/app/mass-scrape/cities-phase1.json', 'utf8'));
const insCity = db.prepare('INSERT OR IGNORE INTO cities (city_state, city, state) VALUES (?, ?, ?)');
let cityAdded = 0;
db.transaction(() => {
  for (const c of cs) {
    const cs2 = c.city_state || `${c.city}, ${c.state}`;
    const r = insCity.run(cs2, c.city, c.state || '');
    if (r.changes) cityAdded++;
  }
})();
const cityCount = db.prepare('SELECT COUNT(*) AS c FROM cities').get().c;
console.log(`cities: total=${cityCount} new=${cityAdded}`);

console.log('--- settings ---');
const setS = db.prepare('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value');
setS.run('dispatch_enabled', '1');
setS.run('jobs_per_node', '1000');
setS.run('mass_scrape_enabled', '0'); // disable old orchestrator
if (!db.prepare('SELECT 1 FROM settings WHERE key=?').get('dispatch_cursor')) {
  setS.run('dispatch_cursor', '0');
}

const settings = db.prepare("SELECT key,value FROM settings WHERE key IN ('dispatch_enabled','jobs_per_node','mass_scrape_enabled','dispatch_cursor')").all();
console.log(settings);

console.log(`total jobs space: ${indCount * cityCount}`);
console.log('bootstrap done.');
db.close();
