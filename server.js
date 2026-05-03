const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const db = require('./db');
const massSeed = require('./mass-scrape/seed');
const massScheduler = require('./mass-scrape/scheduler');
const ghlSync = require('./ghl-sync');
const aiAgent = require('./ai-agent');

const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));

const PORT = process.env.PORT || 3000;
const ADMIN_PASSWORD = '2TQmwZePcT5k-PBcpY8k4YwPEG3dtNzP';

function now() { return Math.floor(Date.now() / 1000); }

const US_STATES = new Set(['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC']);
function isUSState(state) { return US_STATES.has((state || '').toString().toUpperCase()); }

// ---- admin auth middleware ----
function requireAdmin(req, res, next) {
  const auth = req.headers['x-admin-token'] || req.headers['x-password'] || req.query.token || req.query.password;
  if (auth !== ADMIN_PASSWORD) return res.status(401).json({ error: 'unauthorized' });
  next();
}

// ---- worker auth middleware ----
// No license required — any machine can join the fleet. Just need machine_id for tracking.
function requireWorker(req, res, next) {
  const machineId = req.headers['x-machine-id'] || req.body?.machine_id;
  if (!machineId) return res.status(400).json({ error: 'missing machine_id' });
  req.machineId = machineId;
  next();
}

// ==================== WORKER ENDPOINTS ====================

// Node registers / checks in. Creates row on first contact, updates on repeat.
app.post('/api/fleet/heartbeat', requireWorker, (req, res) => {
  const { hostname, os, app_version, cpu_pct, ram_pct, cpu_cap, ram_cap, label,
          current_job_leads, current_job_industry, current_job_city } = req.body || {};
  const t = now();

  let node = db.prepare('SELECT * FROM nodes WHERE machine_id = ?').get(req.machineId);

  if (!node) {
    db.prepare(`
      INSERT INTO nodes (machine_id, license_key, hostname, label, os, app_version,
        cpu_pct, ram_pct, cpu_cap, ram_cap, status, last_seen, first_seen)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'idle', ?, ?)
    `).run(
      req.machineId, 'open', hostname || null, label || hostname || null,
      os || null, app_version || null,
      cpu_pct ?? 0, ram_pct ?? 0, cpu_cap ?? 50, ram_cap ?? 50, t, t
    );
    node = db.prepare('SELECT * FROM nodes WHERE machine_id = ?').get(req.machineId);
  } else {
    db.prepare(`
      UPDATE nodes
      SET hostname = COALESCE(?, hostname),
          os = COALESCE(?, os),
          app_version = COALESCE(?, app_version),
          cpu_pct = ?, ram_pct = ?,
          cpu_cap = COALESCE(?, cpu_cap),
          ram_cap = COALESCE(?, ram_cap),
          current_job_leads = ?,
          current_job_industry = ?,
          current_job_city = ?,
          status = CASE WHEN paused = 1 THEN 'paused'
                        WHEN current_job_id IS NOT NULL THEN 'working'
                        ELSE 'idle' END,
          last_seen = ?
      WHERE id = ?
    `).run(hostname || null, os || null, app_version || null,
      cpu_pct ?? node.cpu_pct, ram_pct ?? node.ram_pct,
      cpu_cap, ram_cap,
      +(current_job_leads||0),
      current_job_industry || null,
      current_job_city || null,
      t, node.id);
  }

  // Deliver pending commands
  const cmds = db.prepare(`
    SELECT id, kind, payload FROM commands
    WHERE node_id = ? AND delivered = 0 ORDER BY id ASC LIMIT 10
  `).all(node.id);
  if (cmds.length) {
    const ids = cmds.map(c => c.id);
    db.prepare(`UPDATE commands SET delivered = 1 WHERE id IN (${ids.map(() => '?').join(',')})`).run(...ids);
  }

  res.json({
    ok: true,
    node_id: node.id,
    paused: !!node.paused,
    cpu_cap: node.cpu_cap,
    ram_cap: node.ram_cap,
    commands: cmds.map(c => ({ id: c.id, kind: c.kind, payload: c.payload ? JSON.parse(c.payload) : null }))
  });
});

// Worker pulls the next available job.
app.post('/api/fleet/pull-job', requireWorker, (req, res) => {
  const node = db.prepare('SELECT * FROM nodes WHERE machine_id = ?').get(req.machineId);
  if (!node) return res.status(404).json({ error: 'node not registered' });
  if (node.paused) return res.json({ job: null, reason: 'paused' });
  if (node.current_job_id) {
    const j = db.prepare('SELECT * FROM jobs WHERE id = ?').get(node.current_job_id);
    if (j && j.status === 'running') return res.json({ job: j, resumed: true });
  }

  // Atomic claim: start transaction, SELECT queued job, flip to running.
  // Prevents two workers from grabbing the same job in a race.
  let job = null;
  const claimTxn = db.transaction(() => {
    const j = db.prepare(`
      SELECT * FROM jobs
      WHERE status = 'queued' AND (target_node_id IS NULL OR target_node_id = ?)
      ORDER BY priority DESC, id ASC LIMIT 1
    `).get(node.id);
    if (!j) return null;
    const t = now();
    db.prepare(`
      UPDATE jobs SET status='running', assigned_node_id=?, assigned_at=?, started_at=? WHERE id=?
    `).run(node.id, t, t, j.id);
    db.prepare(`UPDATE nodes SET current_job_id=?, status='working' WHERE id=?`).run(j.id, node.id);
    return j;
  });
  job = claimTxn();

  if (!job) return res.json({ job: null });

  // Parse cities JSON for batch jobs; fall back to single city for legacy jobs
  let jobOut = { ...job, status: 'running', assigned_node_id: node.id };
  if (job.cities) {
    try { jobOut.cities = JSON.parse(job.cities); } catch { jobOut.cities = [job.city]; }
  } else {
    jobOut.cities = [job.state ? `${job.city}, ${job.state}` : job.city];
  }
  res.json({ job: jobOut });
});

// Worker returns results for a job.
app.post('/api/fleet/job-result', requireWorker, (req, res) => {
  const { job_id, leads = [], error = null } = req.body || {};
  if (!job_id) return res.status(400).json({ error: 'missing job_id' });

  const node = db.prepare('SELECT * FROM nodes WHERE machine_id = ?').get(req.machineId);
  if (!node) return res.status(404).json({ error: 'node not registered' });
  const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(job_id);
  if (!job) return res.status(404).json({ error: 'job not found' });

  const t = now();
  const insertLead = db.prepare(`
    INSERT OR IGNORE INTO leads (job_id, node_id, name, phone, email, website, address, city, state,
      industry, gcid, search_term, google_category_raw, rating, reviews,
      website_platform, website_status, tags, business_hours,
      reviews_1star, reviews_2star, reviews_3star, reviews_4star, reviews_5star,
      ai_seo_score, ai_design_score, ai_seo_notes, ai_design_notes, ai_analyzed_at, ai_provider,
      created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  let skipped = 0;
  const txn = db.transaction(() => {
    for (const l of leads) {
      // Defense-in-depth: never persist a lead without a business name —
      // older workers had a field-mapping bug (scraper emits business_name,
      // worker was reading l.name) that poisoned thousands of rows.
      const nm = (l.name || l.business_name || '').toString().trim();
      if (!nm) { skipped++; continue; }
      const leadState = (l.state || job.state || '').toString().trim();
      if (!isUSState(leadState)) { skipped++; continue; }
      const baseTags = Array.isArray(l.tags) ? l.tags : (typeof l.tags === 'string' ? JSON.parse(l.tags) : []);
      const industryTag = (l.industry || '').toLowerCase().replace(/\s+/g, '-');
      const tagsSet = new Set(baseTags);
      if (industryTag) tagsSet.add(industryTag);
      const tagsJson = JSON.stringify([...tagsSet]);
      insertLead.run(job.id, node.id,
        nm, l.phone || null, l.email || null, l.website || null,
        l.address || null, l.city || job.city, l.state || job.state,
        l.industry || null, l.gcid || null, l.search_term || job.industry,
        l.google_category_raw || null, l.rating || null, l.reviews || null,
        l.website_platform || null, l.website_status || 'unchecked', tagsJson, l.business_hours || null,
        +(l.reviews_1star||0), +(l.reviews_2star||0), +(l.reviews_3star||0), +(l.reviews_4star||0), +(l.reviews_5star||0),
        l.ai_seo_score == null ? null : +l.ai_seo_score,
        l.ai_design_score == null ? null : +l.ai_design_score,
        l.ai_seo_notes || null, l.ai_design_notes || null,
        l.ai_analyzed_at || null, l.ai_provider || null,
        t);
    }
    db.prepare(`
      UPDATE jobs SET status=?, finished_at=?, leads_found=?, error=? WHERE id=?
    `).run(error ? 'failed' : 'done', t, leads.length, error, job.id);
    db.prepare(`
      UPDATE nodes SET current_job_id=NULL,
                       status='idle',
                       jobs_done=jobs_done+1,
                       leads_harvested=leads_harvested+?
      WHERE id=?
    `).run(leads.length, node.id);
    // Mass-scrape coverage: if this job came from the orchestrator, flip the row
    db.prepare(
      `UPDATE scrape_coverage
       SET status = CASE WHEN ? IS NULL THEN 'done' ELSE 'failed' END,
           leads_found = ?,
           last_error = ?,
           completed_at = ?
       WHERE job_id = ?`
    ).run(error, leads.length, error, t, job.id);
  });
  txn();

  res.json({ ok: true, saved: leads.length - skipped, skipped });
});

// Worker pushes log lines (activity stream). Accepts batch of {msg, job_id, level, t}.
app.post('/api/fleet/log', requireWorker, (req, res) => {
  const { lines = [] } = req.body || {};
  const node = db.prepare('SELECT id FROM nodes WHERE machine_id = ?').get(req.machineId);
  if (!node) return res.status(404).json({ error: 'node not registered' });
  const ins = db.prepare(`INSERT INTO node_logs (node_id, job_id, level, msg, t) VALUES (?, ?, ?, ?, ?)`);
  const t = now();
  const txn = db.transaction(() => {
    for (const ln of lines) {
      if (!ln || !ln.msg) continue;
      ins.run(node.id, ln.job_id || null, ln.level || 'info', String(ln.msg).slice(0, 1000), ln.t || t);
    }
  });
  txn();
  res.json({ ok: true, saved: lines.length });
});

// Admin: fetch recent logs for a node
app.get('/api/admin/nodes/:id/logs', requireAdmin, (req, res) => {
  const id = +req.params.id;
  const limit = Math.min(+req.query.limit || 200, 500);
  const since = req.query.since ? +req.query.since : 0;
  const rows = db.prepare(`
    SELECT id, job_id, level, msg, t FROM node_logs
    WHERE node_id = ? AND id > ?
    ORDER BY id DESC LIMIT ?
  `).all(id, since, limit);
  res.json({ logs: rows.reverse() });
});

// Worker pushes leads scraped LOCALLY (not via fleet job) to the master DB.
// Dedupes on (phone, industry). Accepts up to 1000 leads per call.
app.post('/api/fleet/sync-leads', requireWorker, (req, res) => {
  const { leads = [] } = req.body || {};
  if (!Array.isArray(leads) || !leads.length) return res.json({ ok: true, inserted: 0, skipped: 0 });

  const node = db.prepare('SELECT id FROM nodes WHERE machine_id = ?').get(req.machineId);
  const nodeId = node?.id || null;

  // Use INSERT OR IGNORE against unique (phone, industry) to dedupe across fleet
  const ins = db.prepare(`
    INSERT OR IGNORE INTO leads
      (job_id, node_id, name, phone, email, website, address, city, state,
       industry, gcid, search_term, google_category_raw, rating, reviews,
       website_platform, website_status, tags, business_hours,
       reviews_1star, reviews_2star, reviews_3star, reviews_4star, reviews_5star,
       ai_seo_score, ai_design_score, ai_seo_notes, ai_design_notes, ai_analyzed_at, ai_provider,
       ghl_synced, ghl_contact_id, created_at)
    VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const t = now();
  let inserted = 0, skipped = 0;
  const txn = db.transaction(() => {
    for (const l of leads) {
      if (!isUSState(l.state)) { skipped++; continue; }
      const tagsJson = Array.isArray(l.tags) ? JSON.stringify(l.tags) : (typeof l.tags === 'string' ? l.tags : '[]');
      const r = ins.run(nodeId,
        l.name || null, l.phone || null, l.email || null, l.website || null,
        l.address || null, l.city || null, l.state || null,
        l.industry || null, l.gcid || null, l.search_term || l.industry || null,
        l.google_category_raw || null, l.rating || null, l.reviews || null,
        l.website_platform || null, l.website_status || 'unchecked', tagsJson, l.business_hours || null,
        +(l.reviews_1star||0), +(l.reviews_2star||0), +(l.reviews_3star||0), +(l.reviews_4star||0), +(l.reviews_5star||0),
        l.ai_seo_score == null ? null : +l.ai_seo_score,
        l.ai_design_score == null ? null : +l.ai_design_score,
        l.ai_seo_notes || null, l.ai_design_notes || null,
        l.ai_analyzed_at || null, l.ai_provider || null,
        l.ghl_synced == null ? 0 : +l.ghl_synced,
        l.ghl_contact_id || null,
        t);
      if (r.changes) inserted++; else skipped++;
    }
    if (inserted) {
      db.prepare(`UPDATE nodes SET leads_harvested=leads_harvested+? WHERE id=?`).run(inserted, nodeId);
    }
  });
  txn();
  res.json({ ok: true, inserted, skipped });
});

// Worker pulls master leads (reverse mirror). Every connected install gets a
// copy of the master lead book via this paginated endpoint.
// since_id = worker's highest seen master id; pull in ascending order.
app.get('/api/fleet/master-leads/pull', requireWorker, (req, res) => {
  const sinceId = Math.max(0, +req.query.since_id || 0);
  const limit = Math.min(Math.max(+req.query.limit || 500, 50), 2000);
  const cols = ['id','name','phone','email','website','address','city','state',
    'industry','gcid','search_term','google_category_raw','rating','reviews',
    'website_platform','website_status','tags','business_hours',
    'reviews_1star','reviews_2star','reviews_3star','reviews_4star','reviews_5star',
    'ai_seo_score','ai_design_score','ai_seo_notes','ai_design_notes','ai_analyzed_at','ai_provider',
    'ghl_synced','ghl_contact_id','created_at'];
  const rows = db.prepare(
    `SELECT ${cols.join(',')} FROM leads WHERE id > ? ORDER BY id ASC LIMIT ?`
  ).all(sinceId, limit);
  const nextId = rows.length ? rows[rows.length - 1].id : sinceId;
  const hasMore = rows.length === limit;
  res.json({ leads: rows, next_id: nextId, has_more: hasMore, returned: rows.length });
});

// ==================== ADMIN / DASHBOARD ENDPOINTS ====================

app.get('/api/admin/nodes', requireAdmin, (req, res) => {
  const nodes = db.prepare(`SELECT * FROM nodes ORDER BY last_seen DESC`).all();
  res.json({ nodes });
});

app.get('/api/admin/jobs', requireAdmin, (req, res) => {
  const { status, limit = 100 } = req.query;
  const rows = status
    ? db.prepare(`SELECT * FROM jobs WHERE status=? ORDER BY id DESC LIMIT ?`).all(status, +limit)
    : db.prepare(`SELECT * FROM jobs ORDER BY id DESC LIMIT ?`).all(+limit);
  res.json({ jobs: rows });
});

app.get('/api/admin/stats', requireAdmin, (req, res) => {
  const nodes = db.prepare(`
    SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN status='working' THEN 1 ELSE 0 END) AS working,
      SUM(CASE WHEN status='idle' THEN 1 ELSE 0 END) AS idle,
      SUM(CASE WHEN status='offline' THEN 1 ELSE 0 END) AS offline,
      SUM(CASE WHEN status='paused' THEN 1 ELSE 0 END) AS paused
    FROM nodes`).get();
  const jobs = db.prepare(`
    SELECT
      SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued,
      SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running,
      SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done,
      SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed
    FROM jobs`).get();
  const leads = db.prepare(`SELECT COUNT(*) AS total FROM leads`).get();
  const today = db.prepare(`
    SELECT COUNT(*) AS c FROM leads WHERE created_at > ?
  `).get(Math.floor(Date.now() / 1000) - 86400);
  res.json({ nodes, jobs, leads_total: leads.total, leads_24h: today.c });
});

// Dispatch jobs. Batches cities into multi-city jobs (default 50 per job) so workers
// can scrape concurrently instead of doing 1 city per job.
app.post('/api/admin/dispatch', requireAdmin, (req, res) => {
  const { industry, cities = [], state, target_node_id = null, priority = 0, max_results = 20, batch_size = 50 } = req.body || {};
  if (!industry || !cities.length) return res.status(400).json({ error: 'industry + cities required' });

  // Parse city strings, handling "City, ST" format
  const parsed = [];
  for (const city of cities) {
    let c = city, s = state || null;
    if (typeof city === 'string' && city.includes(',')) {
      const parts = city.split(',').map(x => x.trim());
      c = parts[0];
      s = parts[1] || s;
    }
    parsed.push({ city: c, state: s });
  }

  const ins = db.prepare(`
    INSERT INTO jobs (industry, city, cities, state, target_node_id, priority, status, max_results)
    VALUES (?, ?, ?, ?, ?, ?, 'queued', ?)
  `);
  let count = 0;
  const bs = Math.max(1, Math.min(+batch_size || 50, 200));
  const txn = db.transaction(() => {
    for (let i = 0; i < parsed.length; i += bs) {
      const batch = parsed.slice(i, i + bs);
      const first = batch[0];
      const cityList = JSON.stringify(batch.map(b => b.state ? `${b.city}, ${b.state}` : b.city));
      ins.run(industry, first.city, cityList, first.state, target_node_id || null, priority, +max_results || 20);
      count++;
    }
  });
  txn();
  res.json({ ok: true, dispatched: count, cities: parsed.length });
});

// Broadcast: pause / resume / kill every node
app.post('/api/admin/broadcast/:action', requireAdmin, (req, res) => {
  const action = req.params.action;
  if (!['pause','resume','kill'].includes(action)) return res.status(400).json({ error: 'bad action' });
  const nodes = db.prepare(`SELECT id FROM nodes WHERE status != 'offline'`).all();
  const ins = db.prepare(`INSERT INTO commands (node_id, kind, payload) VALUES (?, ?, NULL)`);
  const upd = db.prepare(`UPDATE nodes SET paused=? WHERE id=?`);
  const txn = db.transaction(() => {
    for (const n of nodes) {
      ins.run(n.id, action);
      if (action === 'pause') upd.run(1, n.id);
      if (action === 'resume') upd.run(0, n.id);
    }
  });
  txn();
  res.json({ ok: true, affected: nodes.length });
});

// Cancel all queued jobs (deletes them)
app.post('/api/admin/jobs/cancel-all', requireAdmin, (req, res) => {
  const r = db.prepare(`DELETE FROM jobs WHERE status='queued'`).run();
  res.json({ ok: true, cancelled: r.changes });
});

// Clear finished jobs (done/failed/cancelled) — housekeeping
app.post('/api/admin/jobs/clear-finished', requireAdmin, (req, res) => {
  const r = db.prepare(`DELETE FROM jobs WHERE status IN ('done','failed','cancelled')`).run();
  res.json({ ok: true, deleted: r.changes });
});

// Search / filter leads
app.get('/api/admin/leads/search', requireAdmin, (req, res) => {
  const { q = '', industry, gcid, state, city, ghlSynced, hasPhone, tags, limit = 100, offset = 0 } = req.query;
  const where = [];
  const params = [];

  // Reject very short text searches to avoid full-table scans
  const qTrim = String(q).trim();
  if (qTrim.length === 1) {
    return res.json({ leads: [], total: 0, note: 'Search requires at least 2 characters' });
  }

  if (qTrim) {
    // If query looks like a phone number (digits, spaces, dashes, parens),
    // use prefix search on phone index for speed.
    const isPhoneLike = /^[\d\s\-\(\)\+\.]+$/.test(qTrim);
    if (isPhoneLike) {
      where.push(`phone LIKE ?`);
      params.push(`${qTrim}%`);
    } else {
      where.push(`(name LIKE ? OR phone LIKE ? OR email LIKE ? OR website LIKE ? OR address LIKE ?)`);
      const qq = `%${qTrim}%`;
      params.push(qq, qq, qq, qq, qq);
    }
  }
  if (industry) { where.push('industry = ?'); params.push(industry); }
  if (gcid)     { where.push('gcid = ?');     params.push(gcid); }
  if (state)    { where.push('state = ?');    params.push(state); }
  if (city)     { where.push('city LIKE ?');  params.push(`%${city}%`); }
  if (ghlSynced === 'true')  where.push(`ghl_synced = 1`);
  if (ghlSynced === 'false') where.push(`(ghl_synced IS NULL OR ghl_synced = 0)`);
  if (hasPhone === 'true')   where.push(`phone IS NOT NULL AND phone != ''`);
  if (tags)     { where.push(`tags LIKE ?`);  params.push(`%"${tags}"%`); }

  const whereClause = where.length ? 'WHERE '+where.join(' AND ') : '';

  // Fetch only columns the UI needs — SELECT * pulls large TEXT fields (ai_seo_notes,
  // ai_design_notes, etc.) which slows I/O on 475k+ rows.
  const selectCols = 'id,name,phone,email,website,website_platform,website_status,tags,city,state,industry,rating,reviews,ai_seo_score,ai_design_score,created_at';

  // Fetch limit+1 so we know if there's a next page without an expensive COUNT.
  const fetchLimit = +limit + 1;
  const sql = `SELECT ${selectCols} FROM leads ${whereClause} ORDER BY id DESC LIMIT ? OFFSET ?`;
  params.push(fetchLimit, +offset);
  const rows = db.prepare(sql).all(...params);
  const hasMore = rows.length > limit;
  if (hasMore) rows.pop(); // trim the extra row

  // Accurate total count for all filters (needed so the UI can show real pagination)
  const totalRow = db.prepare(`SELECT COUNT(*) AS c FROM leads ${whereClause}`).get(...params.slice(0, -2));
  const total = totalRow ? totalRow.c : 0;
  res.json({ leads: rows, total, has_more: hasMore });
});

// CSV export — supports the same filters as /search so the UI can export filtered selections
app.get('/api/admin/leads/export', requireAdmin, (req, res) => {
  const { q = '', industry, gcid, state, city, ghlSynced, hasPhone, tags, limit = 100000 } = req.query;
  const where = [];
  const params = [];

  const qTrim = String(q).trim();
  if (qTrim.length === 1) {
    return res.status(400).json({ error: 'Search requires at least 2 characters' });
  }
  if (qTrim) {
    const isPhoneLike = /^[\d\s\-\(\)\+\.]+$/.test(qTrim);
    if (isPhoneLike) {
      where.push(`phone LIKE ?`);
      params.push(`${qTrim}%`);
    } else {
      where.push(`(name LIKE ? OR phone LIKE ? OR email LIKE ? OR website LIKE ? OR address LIKE ?)`);
      const qq = `%${qTrim}%`;
      params.push(qq, qq, qq, qq, qq);
    }
  }
  if (industry) { where.push('industry = ?'); params.push(industry); }
  if (gcid)     { where.push('gcid = ?');     params.push(gcid); }
  if (state)    { where.push('state = ?');    params.push(state); }
  if (city)     { where.push('city LIKE ?');  params.push(`%${city}%`); }
  if (ghlSynced === 'true')  where.push(`ghl_synced = 1`);
  if (ghlSynced === 'false') where.push(`(ghl_synced IS NULL OR ghl_synced = 0)`);
  if (hasPhone === 'true')   where.push(`phone IS NOT NULL AND phone != ''`);
  if (tags)     { where.push(`tags LIKE ?`);  params.push(`%"${tags}"%`); }

  const whereClause = where.length ? 'WHERE '+where.join(' AND ') : '';
  const rows = db.prepare(`SELECT * FROM leads ${whereClause} ORDER BY id DESC LIMIT ?`).all(...params, +limit);
  const cols = ['name','phone','email','website','address','city','state','industry','gcid','rating','reviews','website_platform','website_status','tags','business_hours','reviews_1star','reviews_2star','reviews_3star','reviews_4star','reviews_5star','ai_seo_score','ai_design_score','ai_seo_notes','ai_design_notes','ai_provider','ai_analyzed_at'];
  const esc = v => v == null ? '' : `"${String(v).replace(/"/g, '""')}"`;
  let csv = cols.join(',') + '\n';
  for (const r of rows) csv += cols.map(c => esc(r[c])).join(',') + '\n';
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', `attachment; filename="fleet-leads-${Date.now()}.csv"`);
  res.send(csv);
});

// Update a single lead
app.patch('/api/admin/leads/:id', requireAdmin, (req, res) => {
  const id = +req.params.id;
  const allowed = ['name','phone','email','website','address','city','state','industry','gcid','rating','reviews','website_platform','website_status','tags','business_hours','reviews_1star','reviews_2star','reviews_3star','reviews_4star','reviews_5star','ai_seo_score','ai_design_score','ai_seo_notes','ai_design_notes','ai_provider'];
  const fields = [];
  const vals = [];
  for (const [k, v] of Object.entries(req.body || {})) {
    if (allowed.includes(k)) {
      fields.push(`${k}=?`);
      vals.push(v);
    }
  }
  if (!fields.length) return res.json({ ok: true });
  vals.push(id);
  db.prepare(`UPDATE leads SET ${fields.join(', ')} WHERE id=?`).run(...vals);
  res.json({ ok: true });
});

// CSV / JSON import — mirrors export format
app.post('/api/admin/leads/import', requireAdmin, (req, res) => {
  const { leads = [] } = req.body || {};
  if (!Array.isArray(leads) || !leads.length) return res.status(400).json({ error: 'leads[] required' });
  const ins = db.prepare(`
    INSERT OR IGNORE INTO leads (name, phone, email, website, address, city, state,
      industry, gcid, search_term, google_category_raw, rating, reviews,
      website_platform, website_status, tags, business_hours,
      reviews_1star, reviews_2star, reviews_3star, reviews_4star, reviews_5star,
      ai_seo_score, ai_design_score, ai_seo_notes, ai_design_notes, ai_provider, ai_analyzed_at,
      created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const t = now();
  const toInt = v => (v == null || v === '') ? null : (Number.isFinite(+v) ? +v : null);
  let inserted = 0, skipped = 0;
  const txn = db.transaction(() => {
    for (const l of leads) {
      const baseTags = Array.isArray(l.tags) ? l.tags : (typeof l.tags === 'string' && l.tags.trim().startsWith('[') ? JSON.parse(l.tags) : []);
      const industryTag = (l.industry || '').toLowerCase().replace(/\s+/g, '-');
      const tagsSet = new Set(baseTags);
      if (industryTag) tagsSet.add(industryTag);
      const tagsJson = JSON.stringify([...tagsSet]);
      const r = ins.run(
        l.name || l.business_name || null,
        l.phone || null, l.email || null, l.website || null,
        l.address || null, l.city || null, l.state || null,
        l.industry || null, l.gcid || null,
        l.search_term || l.industry || null,
        l.google_category_raw || null,
        l.rating == null || l.rating === '' ? null : +l.rating,
        l.reviews == null || l.reviews === '' ? null : +l.reviews,
        l.website_platform || null,
        l.website_status || 'unchecked',
        tagsJson,
        l.business_hours || null,
        toInt(l.reviews_1star) || 0,
        toInt(l.reviews_2star) || 0,
        toInt(l.reviews_3star) || 0,
        toInt(l.reviews_4star) || 0,
        toInt(l.reviews_5star) || 0,
        toInt(l.ai_seo_score),
        toInt(l.ai_design_score),
        l.ai_seo_notes || null,
        l.ai_design_notes || null,
        l.ai_provider || null,
        toInt(l.ai_analyzed_at),
        t
      );
      if (r.changes) inserted++; else skipped++;
    }
  });
  txn();
  res.json({ ok: true, inserted, skipped, total: leads.length });
});

// Industry + city metadata (needed by dashboard)
app.get('/api/meta/industries', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'industries.json'));
});
app.get('/api/meta/cities', (req, res) => {
  try {
    const raw = JSON.parse(fs.readFileSync(path.join(__dirname, 'public', 'us-cities.json'), 'utf8'));
    const parsed = raw.map(str => {
      const s = String(str);
      const parts = s.split(',').map(x => x.trim());
      return { city_state: s, city: parts[0] || s, state: parts[1] || '' };
    });
    res.json(parsed);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/admin/jobs/:id/cancel', requireAdmin, (req, res) => {
  const id = +req.params.id;
  const job = db.prepare('SELECT * FROM jobs WHERE id=?').get(id);
  if (!job) return res.status(404).json({ error: 'not found' });
  if (job.status === 'running' && job.assigned_node_id) {
    db.prepare(`INSERT INTO commands (node_id, kind, payload) VALUES (?, 'cancel_job', ?)`)
      .run(job.assigned_node_id, JSON.stringify({ job_id: id }));
    // Free the node so it can pick up new work
    db.prepare(`UPDATE nodes SET current_job_id=NULL, status='idle' WHERE id=?`).run(job.assigned_node_id);
  }
  db.prepare(`DELETE FROM jobs WHERE id=?`).run(id);
  res.json({ ok: true, deleted: 1 });
});

app.post('/api/admin/nodes/:id/pause', requireAdmin, (req, res) => {
  const id = +req.params.id;
  db.prepare(`UPDATE nodes SET paused=1, status='paused' WHERE id=?`).run(id);
  db.prepare(`INSERT INTO commands (node_id, kind, payload) VALUES (?, 'pause', NULL)`).run(id);
  res.json({ ok: true });
});

app.post('/api/admin/nodes/:id/resume', requireAdmin, (req, res) => {
  const id = +req.params.id;
  db.prepare(`UPDATE nodes SET paused=0 WHERE id=?`).run(id);
  db.prepare(`INSERT INTO commands (node_id, kind, payload) VALUES (?, 'resume', NULL)`).run(id);
  res.json({ ok: true });
});

app.post('/api/admin/nodes/:id/kill', requireAdmin, (req, res) => {
  const id = +req.params.id;
  db.prepare(`INSERT INTO commands (node_id, kind, payload) VALUES (?, 'kill', NULL)`).run(id);
  res.json({ ok: true });
});

app.patch('/api/admin/nodes/:id', requireAdmin, (req, res) => {
  const id = +req.params.id;
  const { label, cpu_cap, ram_cap } = req.body || {};
  const fields = [];
  const vals = [];
  if (label !== undefined) { fields.push('label=?'); vals.push(label); }
  if (cpu_cap !== undefined) { fields.push('cpu_cap=?'); vals.push(+cpu_cap); }
  if (ram_cap !== undefined) { fields.push('ram_cap=?'); vals.push(+ram_cap); }
  if (!fields.length) return res.json({ ok: true });
  vals.push(id);
  db.prepare(`UPDATE nodes SET ${fields.join(', ')} WHERE id=?`).run(...vals);
  if (cpu_cap !== undefined || ram_cap !== undefined) {
    db.prepare(`INSERT INTO commands (node_id, kind, payload) VALUES (?, 'update_caps', ?)`)
      .run(id, JSON.stringify({ cpu_cap, ram_cap }));
  }
  res.json({ ok: true });
});

// ==================== DISTRIBUTED RECATEGORIZE ====================
// Admin queues leads for recat → workers pull batches → run normalizer on their
// own CPU cores → post updates back. Normalizer is pure CPU so workers parallelize
// across their logical cores.

// Admin: start a recat session by queuing leads (optionally filtered).
app.post('/api/admin/recat/start', requireAdmin, (req, res) => {
  const { scope = 'all', industry = null, state = null } = req.body || {};
  const where = ["(recat_status IS NULL OR recat_status = 'done')"];
  const params = [];
  if (scope === 'no-gcid') where.push('(gcid IS NULL OR gcid = "")');
  if (scope === 'filter' || industry || state) {
    if (industry) { where.push('industry = ?'); params.push(industry); }
    if (state) { where.push('state = ?'); params.push(state); }
  }
  const r = db.prepare(`UPDATE leads SET recat_status = 'queued' WHERE ${where.join(' AND ')}`).run(...params);
  res.json({ ok: true, queued: r.changes });
});

app.get('/api/admin/recat/status', requireAdmin, (req, res) => {
  const row = db.prepare(`
    SELECT
      SUM(CASE WHEN recat_status='queued' THEN 1 ELSE 0 END) AS queued,
      SUM(CASE WHEN recat_status='done' THEN 1 ELSE 0 END) AS done,
      COUNT(*) AS total
    FROM leads`).get();
  res.json(row);
});

app.post('/api/admin/recat/cancel', requireAdmin, (req, res) => {
  const r = db.prepare(`UPDATE leads SET recat_status = NULL WHERE recat_status = 'queued'`).run();
  res.json({ ok: true, cleared: r.changes });
});

// Worker: pull a batch of queued leads to normalize
app.post('/api/fleet/recat/pull', requireWorker, (req, res) => {
  const { batch_size = 500 } = req.body || {};
  const size = Math.min(Math.max(+batch_size || 500, 10), 2000);
  // Atomic claim: select queued IDs, flip to 'working-<nodeId>' so other workers skip them
  const node = db.prepare('SELECT id FROM nodes WHERE machine_id = ?').get(req.machineId);
  const claim = `working-${node?.id || 0}`;
  const rows = db.prepare(`SELECT id FROM leads WHERE recat_status = 'queued' LIMIT ?`).all(size);
  if (!rows.length) return res.json({ leads: [] });
  const ids = rows.map(r => r.id);
  db.prepare(`UPDATE leads SET recat_status=? WHERE id IN (${ids.map(()=>'?').join(',')}) AND recat_status='queued'`)
    .run(claim, ...ids);
  const leads = db.prepare(`
    SELECT id, name, industry, gcid, google_category_raw, website, address
    FROM leads WHERE id IN (${ids.map(()=>'?').join(',')})
  `).all(...ids);
  res.json({ leads });
});

// Worker: submit recategorized leads
app.post('/api/fleet/recat/submit', requireWorker, (req, res) => {
  const { updates = [] } = req.body || {};
  if (!Array.isArray(updates) || !updates.length) return res.json({ ok: true, updated: 0 });
  const upd = db.prepare(`UPDATE leads SET industry=COALESCE(?, industry), gcid=COALESCE(?, gcid), recat_status='done' WHERE id=?`);
  let updated = 0;
  const txn = db.transaction(() => {
    for (const u of updates) {
      const r = upd.run(u.industry || null, u.gcid || null, +u.id);
      if (r.changes) updated++;
    }
  });
  txn();
  res.json({ ok: true, updated });
});

// Live activity: group recent leads (last N minutes) by node + industry + city
// so we can show what each worker is currently scraping (for local scrapes that
// don't go through the fleet-job dispatcher).
app.get('/api/admin/activity', requireAdmin, (req, res) => {
  const windowSec = Math.min(+req.query.window || 180, 3600); // default 3 min
  const cutoff = now() - windowSec;
  const rows = db.prepare(`
    SELECT l.node_id, l.industry,
           COUNT(*) AS leads_count,
           COUNT(DISTINCT l.city) AS cities_count,
           MIN(l.created_at) AS first_at,
           MAX(l.created_at) AS last_at,
           n.label AS node_label, n.hostname AS node_hostname, n.status AS node_status
    FROM leads l
    LEFT JOIN nodes n ON n.id = l.node_id
    WHERE l.created_at >= ? AND l.job_id IS NULL
    GROUP BY l.node_id, l.industry
    ORDER BY last_at DESC
    LIMIT 50
  `).all(cutoff);
  res.json({ activity: rows, window_seconds: windowSec });
});

app.get('/api/admin/leads', requireAdmin, (req, res) => {
  const { limit = 500, job_id } = req.query;
  const rows = job_id
    ? db.prepare(`SELECT * FROM leads WHERE job_id=? ORDER BY id DESC LIMIT ?`).all(+job_id, +limit)
    : db.prepare(`SELECT * FROM leads ORDER BY id DESC LIMIT ?`).all(+limit);
  res.json({ leads: rows });
});

// ==================== MASS SCRAPE ORCHESTRATOR ====================

app.get('/api/admin/mass-scrape/status', requireAdmin, (req, res) => {
  res.json(massScheduler.stats(db));
});

app.post('/api/admin/mass-scrape/start', requireAdmin, (req, res) => {
  db.prepare(`INSERT OR REPLACE INTO settings (key, value) VALUES ('mass_scrape_enabled', '1')`).run();
  massScheduler.logMs(db, { action: 'start', reason: 'admin' });
  // Kick seed if needed
  const r = massSeed.ensureSeeded(db, { logger: (m) => console.log(m) });
  res.json({ ok: true, seed: r });
});

app.post('/api/admin/mass-scrape/pause', requireAdmin, (req, res) => {
  db.prepare(`INSERT OR REPLACE INTO settings (key, value) VALUES ('mass_scrape_enabled', '0')`).run();
  massScheduler.logMs(db, { action: 'pause', reason: req.body?.reason || 'admin' });
  res.json({ ok: true });
});

app.post('/api/admin/mass-scrape/seed-phase2', requireAdmin, (req, res) => {
  const r = massSeed.seedPhase2(db, { logger: (m) => console.log(m) });
  res.json({ ok: true, ...r });
});

app.post('/api/admin/mass-scrape/retry-failed', requireAdmin, (req, res) => {
  const changed = db.prepare(
    `UPDATE scrape_coverage SET status='pending', last_error=NULL WHERE status='failed'`
  ).run().changes;
  res.json({ ok: true, requeued: changed });
});

app.get('/api/admin/mass-scrape/logs', requireAdmin, (req, res) => {
  const limit = Math.min(+req.query.limit || 100, 500);
  const rows = db.prepare(
    `SELECT * FROM mass_scrape_log ORDER BY id DESC LIMIT ?`
  ).all(limit);
  res.json({ logs: rows });
});

app.get('/api/admin/mass-scrape/settings', requireAdmin, (req, res) => {
  const keys = ['mass_scrape_enabled','mass_scrape_max_inflight_per_worker','mass_scrape_industry_filter','mass_scrape_tick_sec','mass_scrape_ai_monitor_enabled','ai_provider','kimi_api_key','kimi_model','groq_api_key','openai_api_key','anthropic_api_key','xai_api_key'];
  const placeholders = keys.map(() => '?').join(',');
  const rows = db.prepare(`SELECT key, value FROM settings WHERE key IN (${placeholders})`).all(...keys);
  const obj = {};
  for (const r of rows) obj[r.key] = r.value;
  // Mask secret keys for the client
  const mask = v => v ? (v.length <= 10 ? '•'.repeat(v.length) : v.slice(0, 6) + '…' + v.slice(-4)) : '';
  const out = {
    mass_scrape_enabled: obj.mass_scrape_enabled || '0',
    mass_scrape_max_inflight_per_worker: obj.mass_scrape_max_inflight_per_worker || '2',
    mass_scrape_industry_filter: obj.mass_scrape_industry_filter || 'home-services',
    mass_scrape_tick_sec: obj.mass_scrape_tick_sec || '30',
    mass_scrape_ai_monitor_enabled: obj.mass_scrape_ai_monitor_enabled || '0',
    ai_provider: obj.ai_provider || 'kimi',
    ai_model: obj.kimi_model || 'kimi-k2-5',
    kimi_api_key_mask: mask(obj.kimi_api_key || ''),
    kimi_api_key_set: !!obj.kimi_api_key,
    groq_api_key_mask: mask(obj.groq_api_key || ''),
    groq_api_key_set: !!obj.groq_api_key,
    openai_api_key_mask: mask(obj.openai_api_key || ''),
    openai_api_key_set: !!obj.openai_api_key,
    anthropic_api_key_mask: mask(obj.anthropic_api_key || ''),
    anthropic_api_key_set: !!obj.anthropic_api_key,
    xai_api_key_mask: mask(obj.xai_api_key || ''),
    xai_api_key_set: !!obj.xai_api_key,
  };
  res.json({ settings: out });
});

app.patch('/api/admin/mass-scrape/settings', requireAdmin, (req, res) => {
  const allowed = ['mass_scrape_max_inflight_per_worker','mass_scrape_industry_filter','mass_scrape_tick_sec','mass_scrape_ai_monitor_enabled','ai_provider','kimi_api_key','kimi_model','groq_api_key','openai_api_key','anthropic_api_key','xai_api_key'];
  const ins = db.prepare(`INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)`);
  for (const [k, v] of Object.entries(req.body || {})) {
    if (allowed.includes(k)) ins.run(k, String(v));
  }
  res.json({ ok: true });
});

// Schedule the scheduler tick
let SCHEDULER_INTERVAL_MS = 30 * 1000;
function rearmScheduler() {
  try {
    const s = +(db.prepare(`SELECT value FROM settings WHERE key='mass_scrape_tick_sec'`).get()?.value || 30);
    SCHEDULER_INTERVAL_MS = Math.max(5, s) * 1000;
  } catch {}
}
setInterval(() => {
  try {
    massScheduler.tick(db, { logger: (m) => console.log(m), seedMod: massSeed });
  } catch (e) { console.error('[mass-scrape] tick error:', e.message); }
  rearmScheduler();
}, 30 * 1000);

// Hourly: requeue rows failed >24h ago (capped to 1 retry)
setInterval(() => {
  try {
    const n = massScheduler.requeueFailed(db);
    if (n > 0) console.log(`[mass-scrape] requeued ${n} failed rows`);
  } catch {}
}, 60 * 60 * 1000);

// Seed on boot (idempotent)
try {
  const r = massSeed.ensureSeeded(db, { logger: (m) => console.log(m) });
  if (r.inserted > 0) console.log(`[mass-scrape boot] seeded ${r.inserted} rows`);
} catch (e) { console.error('[mass-scrape boot] seed error:', e.message); }

// ==================== JOB TIMEOUT SWEEPER ====================
// Jobs that have been running for >10 minutes without finishing are assumed
// stuck (crashed worker, network issue, etc.). Reset to queued so another
// worker can pick them up.
const JOB_TIMEOUT_SECONDS = 600;
setInterval(() => {
  try {
    const cutoff = now() - JOB_TIMEOUT_SECONDS;
    const tx = db.transaction(() => {
      const stuck = db.prepare(`
        SELECT j.id, j.assigned_node_id
        FROM jobs j
        JOIN nodes n ON n.id = j.assigned_node_id
        WHERE j.status = 'running' AND j.started_at < ?
      `).all(cutoff);
      for (const row of stuck) {
        db.prepare(`UPDATE jobs SET status='queued', assigned_node_id=NULL, started_at=NULL WHERE id=?`).run(row.id);
        db.prepare(`UPDATE nodes SET current_job_id=NULL, status='idle' WHERE id=? AND current_job_id=?`).run(row.assigned_node_id, row.id);
        console.log(`[job-timeout] reset stuck job ${row.id} (node ${row.assigned_node_id})`);
      }
    });
    tx();
  } catch (e) { console.error('[job-timeout] sweeper error:', e.message); }
}, 60000);

// ==================== GHL SYNC ====================

app.get('/api/admin/ghl/config', requireAdmin, (req, res) => {
  const c = ghlSync.getCreds(db);
  res.json({
    hasKey: !!c.apiKey,
    locationId: c.locationId,
    keyMask: c.apiKey ? (c.apiKey.slice(0, 6) + '…' + c.apiKey.slice(-4)) : '',
  });
});

app.post('/api/admin/ghl/config', requireAdmin, (req, res) => {
  const { apiKey, locationId } = req.body || {};
  ghlSync.saveCreds(db, { apiKey, locationId });
  res.json({ ok: true });
});

app.get('/api/admin/ghl/status', requireAdmin, (req, res) => {
  res.json({ status: ghlSync.status(), stats: ghlSync.stats(db) });
});

app.post('/api/admin/ghl/sync', requireAdmin, async (req, res) => {
  try {
    const { leadIds, filter, onlyUnsynced = true, trigger } = req.body || {};
    const r = await ghlSync.runSync(db, {
      leadIds: Array.isArray(leadIds) ? leadIds.map(Number).filter(Boolean) : null,
      filter: filter || null,
      onlyUnsynced: !!onlyUnsynced,
      trigger: trigger || 'manual',
    });
    res.json(r);
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.post('/api/admin/ghl/cancel', requireAdmin, (req, res) => {
  ghlSync.cancel();
  res.json({ ok: true });
});

app.post('/api/admin/ghl/reset-sync', requireAdmin, (req, res) => {
  const ids = Array.isArray(req.body?.leadIds) ? req.body.leadIds.map(Number).filter(Boolean) : [];
  const changed = ghlSync.bulkResetSync(db, ids);
  res.json({ ok: true, changed });
});

app.post('/api/admin/ghl/auto-sync', requireAdmin, (req, res) => {
  try {
    ghlSync.startAutoSync(db);
    res.json({ ok: true, running: true });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.post('/api/admin/ghl/auto-sync/stop', requireAdmin, (req, res) => {
  ghlSync.stopAutoSync();
  res.json({ ok: true, running: false });
});

// Admin: batch-convert legacy single-city jobs into multi-city batch jobs.
// Groups queued jobs by industry+state, merges cities into batches of 50.
app.post('/api/admin/jobs/batch-convert', requireAdmin, (req, res) => {
  try {
    const batchSize = Math.max(2, Math.min(+req.body?.batch_size || 50, 200));
    const rows = db.prepare(`SELECT id, industry, city, state FROM jobs WHERE status = 'queued' AND (cities IS NULL OR cities = '') ORDER BY industry, state, id ASC`).all();

    // Group by industry + state
    const groups = new Map();
    for (const r of rows) {
      const key = `${r.industry}||${r.state || ''}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(r);
    }

    let merged = 0, deleted = 0;
    const txn = db.transaction(() => {
      for (const [, jobs] of groups) {
        for (let i = 0; i < jobs.length; i += batchSize) {
          const batch = jobs.slice(i, i + batchSize);
          const first = batch[0];
          const cityList = JSON.stringify(batch.map(j => j.state ? `${j.city}, ${j.state}` : j.city));
          db.prepare(`UPDATE jobs SET cities = ?, city = ? WHERE id = ?`).run(cityList, first.city, first.id);
          merged++;
          // Delete the rest in this batch
          for (let k = 1; k < batch.length; k++) {
            db.prepare(`DELETE FROM jobs WHERE id = ?`).run(batch[k].id);
            deleted++;
          }
        }
      }
    });
    txn();
    res.json({ ok: true, merged, deleted, original_count: rows.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Leads metadata for dropdown filters
app.get('/api/admin/leads/meta', requireAdmin, (req, res) => {
  try {
    const industries = db.prepare(`SELECT DISTINCT industry FROM leads WHERE industry IS NOT NULL AND industry != '' ORDER BY industry ASC LIMIT 2000`).all().map(r => r.industry);
    const states = db.prepare(`SELECT DISTINCT state FROM leads WHERE state IS NOT NULL AND state != '' ORDER BY state ASC LIMIT 200`).all().map(r => r.state);
    const cities = db.prepare(`SELECT DISTINCT city FROM leads WHERE city IS NOT NULL AND city != '' ORDER BY city ASC LIMIT 2000`).all().map(r => r.city);
    // Extract unique tags from JSON arrays
    const tagRows = db.prepare(`SELECT DISTINCT tags FROM leads WHERE tags IS NOT NULL AND tags != '' AND tags != '[]' LIMIT 5000`).all();
    const tagSet = new Set();
    for (const row of tagRows) {
      try {
        const arr = JSON.parse(row.tags);
        if (Array.isArray(arr)) for (const t of arr) if (t) tagSet.add(t);
      } catch {}
    }
    res.json({ industries, states, cities, tags: Array.from(tagSet).sort() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ==================== AI ASSISTANT ====================

app.post('/api/admin/ai/chat', requireAdmin, async (req, res) => {
  try {
    const { history = [], userMessage = '' } = req.body || {};
    const r = await aiAgent.chat(db, { history, userMessage });
    res.json(r);
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// ==================== UPDATES (self-hosted releases) ====================
// Release binaries + latest.json live in /data/releases (Docker volume on Contabo).
// latest.json shape:
//   { "version": "6.6.0",
//     "notes": "…",
//     "assets": {
//       "portable":  "LeadRipper-Portable-v6.6.0.exe",
//       "installer": "LeadRipper-Setup-v6.6.0.exe",
//       "dmg":       "LeadRipper-v6.6.0.dmg"
//     } }
const RELEASES_DIR = path.join(process.env.DATA_DIR || '/data', 'releases');
try { fs.mkdirSync(RELEASES_DIR, { recursive: true }); } catch {}

app.get('/api/updates/latest', (req, res) => {
  try {
    const p = path.join(RELEASES_DIR, 'latest.json');
    if (!fs.existsSync(p)) return res.status(404).json({ error: 'no release published' });
    const meta = JSON.parse(fs.readFileSync(p, 'utf8'));
    // Verify files exist; include size for each asset so the client can show progress
    const assets = {};
    for (const [kind, name] of Object.entries(meta.assets || {})) {
      const fp = path.join(RELEASES_DIR, name);
      if (fs.existsSync(fp)) {
        assets[kind] = { name, size: fs.statSync(fp).size };
      }
    }
    res.json({ version: meta.version, notes: meta.notes || '', assets });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/updates/download/:file', (req, res) => {
  const name = path.basename(req.params.file); // prevent traversal
  if (!/^[A-Za-z0-9._-]+$/.test(name)) return res.status(400).json({ error: 'bad filename' });
  const fp = path.join(RELEASES_DIR, name);
  if (!fs.existsSync(fp)) return res.status(404).json({ error: 'not found' });
  res.sendFile(fp);
});

// Admin-only: upload a new binary + update latest.json atomically.
// Accepts multipart upload isn't worth wiring for a 1/week operation; admin POSTs
// binary bytes as raw body with X-Filename header.
app.post('/api/admin/updates/upload', requireAdmin, express.raw({ type: '*/*', limit: '500mb' }), (req, res) => {
  const filename = req.headers['x-filename'];
  if (!filename || !/^[A-Za-z0-9._-]+$/.test(String(filename))) {
    return res.status(400).json({ error: 'bad X-Filename header' });
  }
  const fp = path.join(RELEASES_DIR, String(filename));
  fs.writeFileSync(fp, req.body);
  res.json({ ok: true, saved: filename, size: req.body.length });
});

app.get('/api/admin/updates/versions', requireAdmin, (req, res) => {
  try {
    let latest = null;
    const latestPath = path.join(RELEASES_DIR, 'latest.json');
    if (fs.existsSync(latestPath)) {
      latest = JSON.parse(fs.readFileSync(latestPath, 'utf8'));
    }
    const files = fs.readdirSync(RELEASES_DIR)
      .filter(name => name !== 'latest.json')
      .map(name => {
        const stat = fs.statSync(path.join(RELEASES_DIR, name));
        return { name, size: stat.size, mtime: stat.mtime.toISOString() };
      })
      .sort((a, b) => b.mtime.localeCompare(a.mtime));
    res.json({ latest, files });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/admin/updates/download/:file', requireAdmin, (req, res) => {
  const name = path.basename(req.params.file);
  if (!/^[A-Za-z0-9._-]+$/.test(name)) return res.status(400).json({ error: 'bad filename' });
  const fp = path.join(RELEASES_DIR, name);
  if (!fs.existsSync(fp)) return res.status(404).json({ error: 'not found' });
  res.sendFile(fp);
});

app.post('/api/admin/updates/publish', requireAdmin, (req, res) => {
  const { version, notes = '', assets = {} } = req.body || {};
  if (!version || typeof assets !== 'object') return res.status(400).json({ error: 'version + assets required' });
  const meta = { version, notes, assets, published_at: now() };
  fs.writeFileSync(path.join(RELEASES_DIR, 'latest.json'), JSON.stringify(meta, null, 2));
  res.json({ ok: true, meta });
});

// ==================== FLEET WORKER RELEASES ====================
const FW_RELEASES_DIR = path.join(RELEASES_DIR, 'fleet-worker');
try { fs.mkdirSync(FW_RELEASES_DIR, { recursive: true }); } catch {}

app.get('/api/updates/fleet-worker/latest', (req, res) => {
  try {
    const p = path.join(FW_RELEASES_DIR, 'latest.json');
    if (!fs.existsSync(p)) return res.status(404).json({ error: 'no release published' });
    const meta = JSON.parse(fs.readFileSync(p, 'utf8'));
    const assets = {};
    for (const [kind, name] of Object.entries(meta.assets || {})) {
      const fp = path.join(FW_RELEASES_DIR, name);
      if (fs.existsSync(fp)) {
        assets[kind] = { name, size: fs.statSync(fp).size };
      }
    }
    res.json({ version: meta.version, notes: meta.notes || '', assets });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/updates/fleet-worker/download/:file', (req, res) => {
  const name = path.basename(req.params.file);
  if (!/^[A-Za-z0-9._-]+$/.test(name)) return res.status(400).json({ error: 'bad filename' });
  const fp = path.join(FW_RELEASES_DIR, name);
  if (!fs.existsSync(fp)) return res.status(404).json({ error: 'not found' });
  res.sendFile(path.resolve(fp));
});

app.post('/api/admin/updates/fleet-worker/upload', requireAdmin, express.raw({ type: '*/*', limit: '500mb' }), (req, res) => {
  const filename = req.headers['x-filename'];
  if (!filename || !/^[A-Za-z0-9._-]+$/.test(String(filename))) {
    return res.status(400).json({ error: 'bad X-Filename header' });
  }
  const fp = path.join(FW_RELEASES_DIR, String(filename));
  fs.writeFileSync(fp, req.body);
  res.json({ ok: true, saved: filename, size: req.body.length });
});

app.get('/api/admin/updates/fleet-worker/versions', requireAdmin, (req, res) => {
  try {
    let latest = null;
    const latestPath = path.join(FW_RELEASES_DIR, 'latest.json');
    if (fs.existsSync(latestPath)) {
      latest = JSON.parse(fs.readFileSync(latestPath, 'utf8'));
    }
    const files = fs.readdirSync(FW_RELEASES_DIR)
      .filter(name => name !== 'latest.json')
      .map(name => {
        const stat = fs.statSync(path.join(FW_RELEASES_DIR, name));
        return { name, size: stat.size, mtime: stat.mtime.toISOString() };
      })
      .sort((a, b) => b.mtime.localeCompare(a.mtime));
    res.json({ latest, files });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/admin/updates/fleet-worker/download/:file', requireAdmin, (req, res) => {
  const name = path.basename(req.params.file);
  if (!/^[A-Za-z0-9._-]+$/.test(name)) return res.status(400).json({ error: 'bad filename' });
  const fp = path.join(FW_RELEASES_DIR, name);
  if (!fs.existsSync(fp)) return res.status(404).json({ error: 'not found' });
  res.sendFile(path.resolve(fp));
});

app.post('/api/admin/updates/fleet-worker/publish', requireAdmin, (req, res) => {
  const { version, notes = '', assets = {} } = req.body || {};
  if (!version || typeof assets !== 'object') return res.status(400).json({ error: 'version + assets required' });
  const meta = { version, notes, assets, published_at: now() };
  fs.writeFileSync(path.join(FW_RELEASES_DIR, 'latest.json'), JSON.stringify(meta, null, 2));
  res.json({ ok: true, meta });
});

// ==================== PUBLIC DOWNLOADS (no auth) ====================
function listReleases(dir) {
  let latest = null;
  const latestPath = path.join(dir, 'latest.json');
  if (fs.existsSync(latestPath)) {
    latest = JSON.parse(fs.readFileSync(latestPath, 'utf8'));
  }
  const files = fs.readdirSync(dir)
    .filter(name => name !== 'latest.json')
    .map(name => {
      const stat = fs.statSync(path.join(dir, name));
      return { name, size: stat.size, mtime: stat.mtime.toISOString() };
    })
    .sort((a, b) => b.mtime.localeCompare(a.mtime));
  return { latest, files };
}

app.get('/api/downloads/versions', (req, res) => {
  try {
    res.json(listReleases(RELEASES_DIR));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/downloads/fleet-worker/versions', (req, res) => {
  try {
    res.json(listReleases(FW_RELEASES_DIR));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/downloads/download/:file', (req, res) => {
  const name = path.basename(req.params.file);
  if (!/^[A-Za-z0-9._-]+$/.test(name)) return res.status(400).json({ error: 'bad filename' });
  const fp = path.join(RELEASES_DIR, name);
  if (!fs.existsSync(fp)) return res.status(404).json({ error: 'not found' });
  res.sendFile(path.resolve(fp));
});

app.get('/api/downloads/fleet-worker/download/:file', (req, res) => {
  const name = path.basename(req.params.file);
  if (!/^[A-Za-z0-9._-]+$/.test(name)) return res.status(400).json({ error: 'bad filename' });
  const fp = path.join(FW_RELEASES_DIR, name);
  if (!fs.existsSync(fp)) return res.status(404).json({ error: 'not found' });
  res.sendFile(path.resolve(fp));
});

// ==================== STATIC ====================
app.use(express.static(path.join(__dirname, 'public')));

app.get('/healthz', (req, res) => res.json({ ok: true, t: now() }));

app.listen(PORT, () => {
  console.log(`[fleet] listening on :${PORT}`);
  console.log(`[fleet] auth: admin password protected, workers open`);
});
