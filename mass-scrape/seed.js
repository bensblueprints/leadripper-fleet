// Seeds scrape_coverage rows from curated industry files × city phase files.
// Called on server startup (idempotent — UNIQUE(industry, city) gates duplicates)
// and when phase1 drains (to seed phase2 on demand).

const fs = require('fs');
const path = require('path');

function readJson(p, fallback) {
  try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch { return fallback; }
}

function loadIndustries(filterName) {
  const file = path.join(__dirname, `${filterName}.json`);
  return readJson(file, []);
}

function loadCities(phase) {
  const file = path.join(__dirname, `cities-phase${phase}.json`);
  const list = readJson(file, null);
  if (list && list.length) return list;

  // Fallback: if phase files are missing, derive them from public/us-cities.json.
  // Phase 1 = top 50 cities per state (by file order, typically alphabetical/size mix).
  // Phase 2 = everything else.
  // This lets mass-scrape run immediately without the SimpleMaps pop-join step.
  const rawPath = path.join(__dirname, '..', 'public', 'us-cities.json');
  const raw = readJson(rawPath, []);
  if (!raw.length) return [];

  const parsed = raw.map(c => {
    const str = typeof c === 'string' ? c : (c.city_state || `${c.city}, ${c.state}`);
    const parts = String(str).split(',').map(s => s.trim());
    return { city_state: str, city: parts[0] || str, state: parts[1] || '' };
  }).filter(x => x.city_state);

  const perStateSeen = new Map();
  const phase1 = [], phase2 = [];
  for (const p of parsed) {
    const n = perStateSeen.get(p.state) || 0;
    if (n < 50) { phase1.push(p); perStateSeen.set(p.state, n + 1); }
    else phase2.push(p);
  }
  return phase === 1 ? phase1 : phase2;
}

function seedPhase(db, { phase, industryFilter, logger = () => {} } = {}) {
  const industries = loadIndustries(industryFilter || 'home-services');
  if (!industries.length) {
    logger(`[mass-scrape seed] no industries in ${industryFilter}.json`);
    return { inserted: 0, total: 0 };
  }
  const cities = loadCities(phase);
  if (!cities.length) {
    logger(`[mass-scrape seed] no cities in cities-phase${phase}.json (run build-phase1.js first)`);
    return { inserted: 0, total: 0 };
  }

  const ins = db.prepare(`
    INSERT OR IGNORE INTO scrape_coverage (industry, gcid, city, state, phase, status)
    VALUES (?, ?, ?, ?, ?, 'pending')
  `);
  let inserted = 0;
  const txn = db.transaction(() => {
    for (const ind of industries) {
      for (const c of cities) {
        const cs = c.city_state || `${c.city}, ${c.state}`;
        const r = ins.run(ind.name, ind.gcid, cs, c.state || '', phase);
        if (r.changes) inserted++;
      }
    }
  });
  txn();
  const total = industries.length * cities.length;
  logger(`[mass-scrape seed] phase=${phase} industries=${industries.length} cities=${cities.length} inserted=${inserted}/${total}`);
  return { inserted, total, industries: industries.length, cities: cities.length };
}

function ensureSeeded(db, { logger = () => {} } = {}) {
  const filter = (db.prepare(`SELECT value FROM settings WHERE key=?`).get('mass_scrape_industry_filter') || {}).value || 'home-services';
  const p1 = db.prepare(`SELECT COUNT(*) AS c FROM scrape_coverage WHERE phase=1`).get().c;
  if (p1 === 0) {
    return seedPhase(db, { phase: 1, industryFilter: filter, logger });
  }
  return { inserted: 0, total: p1, already: true };
}

function seedPhase2(db, { logger = () => {} } = {}) {
  const filter = (db.prepare(`SELECT value FROM settings WHERE key=?`).get('mass_scrape_industry_filter') || {}).value || 'home-services';
  return seedPhase(db, { phase: 2, industryFilter: filter, logger });
}

module.exports = { seedPhase, ensureSeeded, seedPhase2 };
