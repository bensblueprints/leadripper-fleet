// Mass-scrape scheduler tick: matches idle workers to pending coverage rows
// and dispatches them into the existing jobs queue. Reuses the jobs table —
// workers see them as normal fleet jobs and pick them up via pull-job.

function now() { return Math.floor(Date.now() / 1000); }

function getSetting(db, key, fallback) {
  const r = db.prepare(`SELECT value FROM settings WHERE key=?`).get(key);
  return r ? r.value : fallback;
}

function logMs(db, { action = 'info', reason = '', meta = null, level = 'info' } = {}) {
  try {
    db.prepare(`INSERT INTO mass_scrape_log (t, level, action, reason, meta) VALUES (?, ?, ?, ?, ?)`)
      .run(now(), level, action, reason, meta ? JSON.stringify(meta) : null);
  } catch {}
}

function tick(db, { logger = () => {}, seedMod } = {}) {
  try {
    if (getSetting(db, 'mass_scrape_enabled', '0') !== '1') return { skipped: 'disabled' };

    const maxInflightPerWorker = Math.max(1, +getSetting(db, 'mass_scrape_max_inflight_per_worker', '2'));

    // Live workers = seen in last 90s
    const liveCutoff = now() - 90;
    const liveNodes = db.prepare(
      `SELECT id FROM nodes WHERE last_seen > ? AND paused = 0`
    ).all(liveCutoff);
    if (!liveNodes.length) return { skipped: 'no-workers' };

    // How many coverage rows are dispatched-but-not-done?
    const inflight = db.prepare(
      `SELECT COUNT(*) AS c FROM scrape_coverage WHERE status = 'dispatched'`
    ).get().c;
    const maxInflight = liveNodes.length * maxInflightPerWorker;
    const slots = Math.max(0, maxInflight - inflight);
    if (slots === 0) return { dispatched: 0, inflight };

    // Auto-seed phase 2 if phase 1 has no pending rows left
    const p1Pending = db.prepare(
      `SELECT COUNT(*) AS c FROM scrape_coverage WHERE phase=1 AND status='pending'`
    ).get().c;
    if (p1Pending === 0 && seedMod) {
      const p2Pending = db.prepare(
        `SELECT COUNT(*) AS c FROM scrape_coverage WHERE phase=2 AND status='pending'`
      ).get().c;
      if (p2Pending === 0) {
        const r = seedMod.seedPhase2(db, { logger });
        if (r.inserted > 0) {
          logMs(db, { action: 'seed_phase2', reason: `seeded ${r.inserted} rows`, meta: r });
          logger(`[mass-scrape] auto-seeded phase2: ${r.inserted} rows`);
        }
      }
    }

    // Pick N pending coverage rows (phase 1 first)
    const rows = db.prepare(
      `SELECT id, industry, gcid, city, state FROM scrape_coverage
       WHERE status='pending'
       ORDER BY phase ASC, id ASC LIMIT ?`
    ).all(slots);
    if (!rows.length) return { dispatched: 0 };

    const insJob = db.prepare(
      `INSERT INTO jobs (industry, city, state, target_node_id, priority, status, max_results)
       VALUES (?, ?, ?, NULL, 2, 'queued', 200)`
    );
    const updCov = db.prepare(
      `UPDATE scrape_coverage SET status='dispatched', job_id=?, dispatched_at=? WHERE id=?`
    );

    let dispatched = 0;
    const txn = db.transaction(() => {
      for (const r of rows) {
        // Parse "City, ST" into separate fields for the job (existing dispatch expects this)
        const parts = String(r.city).split(',').map(s => s.trim());
        const cityName = parts[0] || r.city;
        const stateCode = parts[1] || r.state || '';
        const result = insJob.run(r.industry, cityName, stateCode);
        updCov.run(result.lastInsertRowid, now(), r.id);
        dispatched++;
      }
    });
    txn();

    if (dispatched > 0) logger(`[mass-scrape] tick dispatched ${dispatched} jobs (workers=${liveNodes.length} inflight_before=${inflight})`);
    return { dispatched, inflight_after: inflight + dispatched, workers: liveNodes.length };
  } catch (e) {
    logger('[mass-scrape] tick error: ' + e.message);
    return { error: e.message };
  }
}

// Failure requeue: any 'failed' coverage row older than 24h → back to 'pending' once.
// We mark it 'pending-retry' so we only retry once per row.
function requeueFailed(db) {
  const cutoff = now() - 86400;
  const r = db.prepare(
    `UPDATE scrape_coverage SET status='pending', last_error=COALESCE(last_error,'')||' [retry]'
     WHERE status='failed' AND (completed_at IS NULL OR completed_at < ?)
       AND (last_error IS NULL OR last_error NOT LIKE '%[retry]%')`
  ).run(cutoff);
  return r.changes;
}

function stats(db) {
  const row = db.prepare(`
    SELECT
      phase,
      status,
      COUNT(*) AS c,
      SUM(leads_found) AS leads
    FROM scrape_coverage
    GROUP BY phase, status
  `).all();

  const byPhase = { 1: { pending: 0, dispatched: 0, done: 0, failed: 0, skipped: 0, leads: 0 },
                    2: { pending: 0, dispatched: 0, done: 0, failed: 0, skipped: 0, leads: 0 } };
  for (const r of row) {
    const p = r.phase || 1;
    if (!byPhase[p]) byPhase[p] = { pending: 0, dispatched: 0, done: 0, failed: 0, skipped: 0, leads: 0 };
    byPhase[p][r.status] = r.c;
    byPhase[p].leads += r.leads || 0;
  }
  const byIndustry = db.prepare(`
    SELECT industry,
      SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
      SUM(CASE WHEN status='dispatched' THEN 1 ELSE 0 END) AS dispatched,
      SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done,
      SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
      SUM(leads_found) AS leads
    FROM scrape_coverage
    GROUP BY industry
    ORDER BY industry
  `).all();

  // ETA: last-hour completion rate
  const hourAgo = now() - 3600;
  const doneLastHour = db.prepare(
    `SELECT COUNT(*) AS c FROM scrape_coverage WHERE status='done' AND completed_at > ?`
  ).get(hourAgo).c;
  const totalPending = (byPhase[1].pending || 0) + (byPhase[2].pending || 0) + (byPhase[1].dispatched || 0) + (byPhase[2].dispatched || 0);
  const etaHours = doneLastHour > 0 ? totalPending / doneLastHour : null;

  return {
    byPhase,
    byIndustry,
    rate_per_hour: doneLastHour,
    eta_hours: etaHours,
    enabled: getSetting(db, 'mass_scrape_enabled', '0') === '1'
  };
}

module.exports = { tick, requeueFailed, stats, logMs, getSetting };
