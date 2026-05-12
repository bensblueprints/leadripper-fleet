// Lead Ripper spec v4 schema migration. Idempotent.
// Run inside container: docker exec leadripper-fleet node /app/migrate.js
const Database = require('better-sqlite3');

const db = new Database('/data/fleet.db');
db.pragma('journal_mode = WAL');

function hasColumn(table, col) {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all();
  return rows.some(r => r.name === col);
}
function hasIndex(name) {
  const r = db.prepare(`SELECT name FROM sqlite_master WHERE type='index' AND name=?`).get(name);
  return !!r;
}
function addCol(table, col, def) {
  if (!hasColumn(table, col)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${col} ${def}`);
    console.log(`+ ${table}.${col}`);
  } else {
    console.log(`= ${table}.${col} (exists)`);
  }
}

console.log('--- column adds ---');
addCol('leads', 'zip_code', 'TEXT');
addCol('leads', 'country', "TEXT DEFAULT 'US'");
addCol('leads', 'business_status', 'TEXT'); // 'open' | 'permanently_closed' | 'temporarily_closed'
addCol('leads', 'timezone', 'TEXT');
addCol('leads', 'contact_name', 'TEXT');
addCol('leads', 'hours_monday', 'TEXT');
addCol('leads', 'hours_tuesday', 'TEXT');
addCol('leads', 'hours_wednesday', 'TEXT');
addCol('leads', 'hours_thursday', 'TEXT');
addCol('leads', 'hours_friday', 'TEXT');
addCol('leads', 'hours_saturday', 'TEXT');
addCol('leads', 'hours_sunday', 'TEXT');
addCol('leads', 'hours_raw', 'TEXT');

// Phone-only global dedup. Spec rule #4: "phone is the unique key, period."
// Existing leads may have multiple rows per phone (kept across industries) — dedupe by min(id).
console.log('--- phone-only dedup ---');
const dupStats = db.prepare(`
  SELECT COUNT(*) AS dup_rows, COUNT(DISTINCT phone) AS unique_phones
  FROM leads
  WHERE phone IS NOT NULL AND phone != ''
    AND id NOT IN (SELECT MIN(id) FROM leads WHERE phone IS NOT NULL AND phone != '' GROUP BY phone)
`).get();
console.log(`will delete ${dupStats.dup_rows} duplicate rows across ${dupStats.unique_phones} phones`);

const dedupTxn = db.transaction(() => {
  const r = db.prepare(`
    DELETE FROM leads
    WHERE phone IS NOT NULL AND phone != ''
      AND id NOT IN (SELECT MIN(id) FROM leads WHERE phone IS NOT NULL AND phone != '' GROUP BY phone)
  `).run();
  console.log(`deleted ${r.changes} duplicate rows`);
});
dedupTxn();

// Drop old composite unique indexes; add phone-only unique.
console.log('--- index swap ---');
if (hasIndex('uniq_leads_phone_industry')) {
  db.exec(`DROP INDEX uniq_leads_phone_industry`);
  console.log('- uniq_leads_phone_industry');
}
if (hasIndex('uniq_leads_phone_industry_city_state')) {
  db.exec(`DROP INDEX uniq_leads_phone_industry_city_state`);
  console.log('- uniq_leads_phone_industry_city_state');
}
if (!hasIndex('uniq_leads_phone')) {
  db.exec(`CREATE UNIQUE INDEX uniq_leads_phone ON leads(phone) WHERE phone IS NOT NULL AND phone != ''`);
  console.log('+ uniq_leads_phone');
} else {
  console.log('= uniq_leads_phone (exists)');
}

// Backfill timezone from state for existing rows.
console.log('--- backfill timezone ---');
const { timezoneForState } = require('/app/timezone.js');
const states = db.prepare(`SELECT DISTINCT state FROM leads WHERE state IS NOT NULL AND (timezone IS NULL OR timezone='')`).all();
const upd = db.prepare(`UPDATE leads SET timezone = ? WHERE state = ? AND (timezone IS NULL OR timezone='')`);
let backfilled = 0;
for (const { state } of states) {
  const tz = timezoneForState(state);
  if (tz) {
    const r = upd.run(tz, state);
    backfilled += r.changes;
  }
}
console.log(`backfilled timezone on ${backfilled} rows`);

// Country backfill — anything with US state stays 'US' (default).
console.log('--- counts ---');
const counts = db.prepare(`SELECT COUNT(*) AS total FROM leads`).get();
console.log(`leads.total = ${counts.total}`);

console.log('migration complete.');
db.close();
