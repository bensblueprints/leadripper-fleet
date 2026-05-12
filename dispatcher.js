// Lazy job dispatcher — replaces the 120M-row scrape_coverage orchestrator.
// Reads small industries + cities lookup tables, walks a cursor (0..N*M),
// dispatches up to `jobs_per_node` (default 1000) inflight jobs to each live
// worker. Only `jobs_completed` rows accumulate (one per finished job),
// preventing the DB from blowing up.

function nowSec() { return Math.floor(Date.now() / 1000); }

function getSetting(db, key, fallback) {
  const r = db.prepare('SELECT value FROM settings WHERE key=?').get(key);
  return r ? r.value : fallback;
}
function setSetting(db, key, value) {
  db.prepare('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
    .run(key, String(value));
}

let _cache = null;
function loadLookup(db, force = false) {
  if (_cache && !force) return _cache;
  const filterTop = getSetting(db, "dispatch_top100_only", "0") === "1";
  const industries = db.prepare(filterTop ? "SELECT id, gcid, name FROM industries WHERE priority=1 ORDER BY id" : "SELECT id, gcid, name FROM industries ORDER BY id").all();
  const cities = db.prepare('SELECT id, city, state FROM cities ORDER BY id').all();
  _cache = { industries, cities };
  return _cache;
}
function invalidateCache() { _cache = null; }

function tick(db, { logger = () => {} } = {}) {
  if (getSetting(db, 'dispatch_enabled', '0') !== '1') return { skipped: 'disabled' };

  const cap = Math.max(1, +getSetting(db, 'jobs_per_node', '1000'));

  const liveCutoff = nowSec() - 90;
  const liveNodes = db.prepare(
    'SELECT id, label FROM nodes WHERE last_seen > ? AND paused = 0'
  ).all(liveCutoff);
  if (!liveNodes.length) return { skipped: 'no-workers' };

  const { industries, cities } = loadLookup(db);
  if (!industries.length || !cities.length) return { skipped: 'lookup-empty' };
  const total = industries.length * cities.length;

  let cursor = +getSetting(db, 'dispatch_cursor', '0');
  if (cursor >= total) {
    logger(`[dispatch] cursor exhausted (${cursor}/${total})`);
    return { skipped: 'exhausted' };
  }

  const inflightStmt = db.prepare(
    `SELECT COUNT(*) AS c FROM jobs
     WHERE target_node_id = ? AND status IN ('queued','running')`
  );
  const completedStmt = db.prepare(
    `SELECT 1 FROM jobs_completed WHERE industry_id = ? AND city_id = ? AND status = 'done'`
  );
  // How many cities to pack into one job so workers can use CITY_CONCURRENCY in parallel.
  // Each job becomes N concurrent scrapes instead of 1. Tunable via settings.
  const citiesPerJob = Math.max(1, Math.min(+getSetting(db, 'cities_per_job', '20'), 200));

  // Single-city job (used when batch collapses to 1 or as fallback)
  const insertJob = db.prepare(
    `INSERT INTO jobs (industry, city, state, target_node_id, priority, status, max_results, industry_id, city_id, created_at)
     VALUES (?, ?, ?, ?, 2, 'queued', 200, ?, ?, ?)`
  );
  // Multi-city batch job — workers process all cities in parallel up to CITY_CONCURRENCY
  const insertBatchJob = db.prepare(
    `INSERT INTO jobs (industry, city, cities, state, target_node_id, priority, status, max_results, industry_id, city_id, created_at)
     VALUES (?, ?, ?, ?, ?, 2, 'queued', 200, ?, ?, ?)`
  );

  let totalDispatched = 0;
  const startCursor = cursor;
  const txn = db.transaction(() => {
    const t = nowSec();
    for (const node of liveNodes) {
      const inflight = inflightStmt.get(node.id).c;
      let slots = cap - inflight;
      if (slots <= 0) continue;

      while (slots > 0 && cursor < total) {
        // City-major traversal: cities vary fastest within each industry.
        // Consecutive cursor positions share the same industry, enabling N-city batch jobs.
        const batchCities = [];
        let batchInd = null;

        while (batchCities.length < citiesPerJob && cursor < total) {
          const cityIdx = cursor % cities.length;
          const indIdx = Math.floor(cursor / cities.length);
          const ind = industries[indIdx];
          const city = cities[cityIdx];
          cursor++;
          if (!ind || !city) continue;
          // Stop batch at industry boundary so each job is a single industry
          if (batchInd && ind.id !== batchInd.id) {
            cursor--; // back up so next outer iteration starts with the new industry
            break;
          }
          batchInd = ind;
          const done = completedStmt.get(ind.id, city.id);
          if (!done) batchCities.push(city);
        }

        if (!batchInd || batchCities.length === 0) continue;

        if (batchCities.length === 1) {
          insertJob.run(batchInd.name, batchCities[0].city, batchCities[0].state, node.id, batchInd.id, batchCities[0].id, t);
        } else {
          const first = batchCities[0];
          const cityList = JSON.stringify(batchCities.map(c => `${c.city}, ${c.state}`));
          insertBatchJob.run(batchInd.name, first.city, cityList, first.state, node.id, batchInd.id, first.id, t);
        }
        slots--;
        totalDispatched++;
      }
    }
    setSetting(db, 'dispatch_cursor', String(cursor));
  });
  txn();

  if (totalDispatched > 0) {
    logger(`[dispatch] tick +${totalDispatched} jobs (${citiesPerJob} cities/job) across ${liveNodes.length} workers (cursor ${startCursor}→${cursor}/${total})`);
  }
  return { dispatched: totalDispatched, workers: liveNodes.length, cursor, total };
}

// Mark a job done in jobs_completed (called from job-result handler).
function markDone(db, { industry_id, city_id, leads_found, node_id, error }) {
  if (!industry_id || !city_id) return;
  db.prepare(`
    INSERT INTO jobs_completed (industry_id, city_id, status, completed_at, leads_found, node_id, last_error)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(industry_id, city_id) DO UPDATE SET
      status = excluded.status,
      completed_at = excluded.completed_at,
      leads_found = excluded.leads_found,
      node_id = excluded.node_id,
      last_error = excluded.last_error
  `).run(industry_id, city_id, error ? 'failed' : 'done', nowSec(), leads_found || 0, node_id, error || null);
}

function stats(db) {
  const total = (() => {
    const r = db.prepare('SELECT COUNT(*) AS c FROM industries').get().c
            * db.prepare('SELECT COUNT(*) AS c FROM cities').get().c;
    return r;
  })();
  const completed = db.prepare("SELECT status, COUNT(*) AS c, COALESCE(SUM(leads_found),0) AS leads FROM jobs_completed GROUP BY status").all();
  const byStatus = { done: 0, failed: 0 };
  let totalLeads = 0;
  for (const r of completed) { byStatus[r.status] = r.c; totalLeads += r.leads; }
  const cursor = +getSetting(db, 'dispatch_cursor', '0');
  const enabled = getSetting(db, 'dispatch_enabled', '0') === '1';
  const cap = +getSetting(db, 'jobs_per_node', '1000');
  const inflight = db.prepare("SELECT COUNT(*) AS c FROM jobs WHERE status IN ('queued','running')").get().c;
  return { total, cursor, completed: byStatus, leads: totalLeads, enabled, cap, inflight };
}

/**
 * Dispatch campaign jobs — one ALL_US job per priority industry.
 * Only runs when setting `dispatch_campaign_mode` = '1'.
 * Each campaign job carries the full sorted city list as a JSON blob;
 * the worker processes them in population-descending order.
 */
function dispatchCampaignJobs(db) {
  if (getSetting(db, 'dispatch_campaign_mode', '0') !== '1') return { skipped: 'campaign_mode_disabled' };

  // All priority industries
  const industries = db.prepare(
    `SELECT id, gcid, name FROM industries WHERE priority = 1 ORDER BY id ASC`
  ).all();
  if (!industries.length) return { skipped: 'no-priority-industries' };

  // All cities sorted by population desc
  const cities = db.prepare(
    `SELECT id, city, state, COALESCE(population, 0) AS population
     FROM cities
     ORDER BY population DESC, city ASC`
  ).all();
  if (!cities.length) return { skipped: 'no-cities' };

  const citiesJson = JSON.stringify(cities);

  const checkExisting = db.prepare(
    `SELECT id FROM jobs WHERE industry = ? AND city = 'ALL_US' AND status IN ('queued', 'running') LIMIT 1`
  );
  const insertJob = db.prepare(
    `INSERT INTO jobs (industry, city, state, cities, priority, status, max_results, industry_id, created_at)
     VALUES (?, 'ALL_US', 'US', ?, 3, 'queued', 200, ?, ?)`
  );

  let dispatched = 0;
  let skippedExisting = 0;

  const txn = db.transaction(() => {
    const t = nowSec();
    for (const ind of industries) {
      const existing = checkExisting.get(ind.name);
      if (existing) { skippedExisting++; continue; }
      insertJob.run(ind.name, citiesJson, ind.id, t);
      dispatched++;
    }
  });
  txn();

  return { dispatched, skipped_existing: skippedExisting, industries: industries.length, cities: cities.length };
}

module.exports = { tick, markDone, stats, invalidateCache, getSetting, setSetting, dispatchCampaignJobs };
