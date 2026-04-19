const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const db = require('./db');

const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));

const PORT = process.env.PORT || 3000;
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || 'change-me-in-coolify';

// Load license hashes from bundled file + optional env override
let LICENSE_HASHES = [];
try {
  const bundled = JSON.parse(fs.readFileSync(path.join(__dirname, 'license-hashes.json'), 'utf8'));
  if (Array.isArray(bundled)) LICENSE_HASHES = LICENSE_HASHES.concat(bundled);
} catch (e) { console.warn('[fleet] no bundled license-hashes.json'); }
if (process.env.LICENSE_HASHES) {
  LICENSE_HASHES = LICENSE_HASHES.concat(
    process.env.LICENSE_HASHES.split(',').map(s => s.trim()).filter(Boolean)
  );
}
LICENSE_HASHES = [...new Set(LICENSE_HASHES)];

function now() { return Math.floor(Date.now() / 1000); }

function hashLicense(key) {
  return crypto.createHash('sha256').update(String(key).trim()).digest('hex');
}

function validLicense(key) {
  if (!key) return false;
  if (LICENSE_HASHES.length === 0) return true; // open mode during setup
  return LICENSE_HASHES.includes(hashLicense(key));
}

// ---- admin auth middleware ----
function requireAdmin(req, res, next) {
  const token = req.headers['x-admin-token'] || req.query.token;
  if (token !== ADMIN_TOKEN) return res.status(401).json({ error: 'unauthorized' });
  next();
}

// ---- worker auth middleware ----
function requireWorker(req, res, next) {
  const key = req.headers['x-license-key'] || req.body?.license_key;
  const machineId = req.headers['x-machine-id'] || req.body?.machine_id;
  if (!validLicense(key)) return res.status(401).json({ error: 'invalid license' });
  if (!machineId) return res.status(400).json({ error: 'missing machine_id' });
  req.licenseKey = key;
  req.machineId = machineId;
  next();
}

// ==================== WORKER ENDPOINTS ====================

// Node registers / checks in. Creates row on first contact, updates on repeat.
app.post('/api/fleet/heartbeat', requireWorker, (req, res) => {
  const { hostname, os, app_version, cpu_pct, ram_pct, cpu_cap, ram_cap, label } = req.body || {};
  const t = now();

  let node = db.prepare('SELECT * FROM nodes WHERE machine_id = ?').get(req.machineId);

  if (!node) {
    db.prepare(`
      INSERT INTO nodes (machine_id, license_key, hostname, label, os, app_version,
        cpu_pct, ram_pct, cpu_cap, ram_cap, status, last_seen, first_seen)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'idle', ?, ?)
    `).run(
      req.machineId, req.licenseKey, hostname || null, label || hostname || null,
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
          status = CASE WHEN paused = 1 THEN 'paused'
                        WHEN current_job_id IS NOT NULL THEN 'working'
                        ELSE 'idle' END,
          last_seen = ?
      WHERE id = ?
    `).run(hostname || null, os || null, app_version || null,
      cpu_pct ?? node.cpu_pct, ram_pct ?? node.ram_pct,
      cpu_cap, ram_cap, t, node.id);
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

  // Prefer jobs targeted at this node, else any queued job
  const job = db.prepare(`
    SELECT * FROM jobs
    WHERE status = 'queued' AND (target_node_id IS NULL OR target_node_id = ?)
    ORDER BY priority DESC, id ASC LIMIT 1
  `).get(node.id);

  if (!job) return res.json({ job: null });

  const t = now();
  db.prepare(`
    UPDATE jobs SET status='running', assigned_node_id=?, assigned_at=?, started_at=? WHERE id=?
  `).run(node.id, t, t, job.id);
  db.prepare(`UPDATE nodes SET current_job_id=?, status='working' WHERE id=?`).run(job.id, node.id);

  res.json({ job: { ...job, status: 'running', assigned_node_id: node.id } });
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

  const txn = db.transaction(() => {
    for (const l of leads) {
      const tagsJson = Array.isArray(l.tags) ? JSON.stringify(l.tags) : (typeof l.tags === 'string' ? l.tags : '[]');
      insertLead.run(job.id, node.id,
        l.name || null, l.phone || null, l.email || null, l.website || null,
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
  });
  txn();

  res.json({ ok: true, saved: leads.length });
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
       created_at)
    VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const t = now();
  let inserted = 0, skipped = 0;
  const txn = db.transaction(() => {
    for (const l of leads) {
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

// Dispatch a job. If cities is an array, fans out to multiple jobs.
app.post('/api/admin/dispatch', requireAdmin, (req, res) => {
  const { industry, cities = [], state, target_node_id = null, priority = 0, max_results = 20 } = req.body || {};
  if (!industry || !cities.length) return res.status(400).json({ error: 'industry + cities required' });

  const ins = db.prepare(`
    INSERT INTO jobs (industry, city, state, target_node_id, priority, status, max_results)
    VALUES (?, ?, ?, ?, ?, 'queued', ?)
  `);
  let count = 0;
  const txn = db.transaction(() => {
    for (const city of cities) {
      // Accept "City, ST" strings too — split them
      let c = city, s = state || null;
      if (typeof city === 'string' && city.includes(',')) {
        const parts = city.split(',').map(x => x.trim());
        c = parts[0];
        s = parts[1] || s;
      }
      ins.run(industry, c, s, target_node_id || null, priority, +max_results || 20);
      count++;
    }
  });
  txn();
  res.json({ ok: true, dispatched: count });
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
  const { q = '', industry, gcid, state, city, limit = 500, offset = 0 } = req.query;
  const where = [];
  const params = [];
  if (q) {
    where.push(`(name LIKE ? OR phone LIKE ? OR email LIKE ? OR website LIKE ? OR address LIKE ?)`);
    const qq = `%${q}%`;
    params.push(qq, qq, qq, qq, qq);
  }
  if (industry) { where.push('industry = ?'); params.push(industry); }
  if (gcid)     { where.push('gcid = ?');     params.push(gcid); }
  if (state)    { where.push('state = ?');    params.push(state); }
  if (city)     { where.push('city LIKE ?');  params.push(`%${city}%`); }
  const sql = `SELECT * FROM leads ${where.length ? 'WHERE '+where.join(' AND '):''} ORDER BY id DESC LIMIT ? OFFSET ?`;
  params.push(+limit, +offset);
  const rows = db.prepare(sql).all(...params);
  const totalRow = db.prepare(`SELECT COUNT(*) AS c FROM leads ${where.length ? 'WHERE '+where.join(' AND '):''}`).get(...params.slice(0, -2));
  res.json({ leads: rows, total: totalRow.c });
});

// CSV export
app.get('/api/admin/leads/export', requireAdmin, (req, res) => {
  const { industry, gcid, state, limit = 100000 } = req.query;
  const where = [];
  const params = [];
  if (industry) { where.push('industry = ?'); params.push(industry); }
  if (gcid)     { where.push('gcid = ?');     params.push(gcid); }
  if (state)    { where.push('state = ?');    params.push(state); }
  const rows = db.prepare(`SELECT * FROM leads ${where.length ? 'WHERE '+where.join(' AND '):''} ORDER BY id DESC LIMIT ?`).all(...params, +limit);
  const cols = ['name','phone','email','website','address','city','state','industry','gcid','rating','reviews','website_platform','website_status','tags','business_hours','reviews_1star','reviews_2star','reviews_3star','reviews_4star','reviews_5star','ai_seo_score','ai_design_score','ai_seo_notes','ai_design_notes','ai_provider','ai_analyzed_at'];
  const esc = v => v == null ? '' : `"${String(v).replace(/"/g, '""')}"`;
  let csv = cols.join(',') + '\n';
  for (const r of rows) csv += cols.map(c => esc(r[c])).join(',') + '\n';
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', `attachment; filename="fleet-leads-${Date.now()}.csv"`);
  res.send(csv);
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
      const tagsJson = Array.isArray(l.tags) ? JSON.stringify(l.tags) : (typeof l.tags === 'string' && l.tags.trim().startsWith('[') ? l.tags : '[]');
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
  res.sendFile(path.join(__dirname, 'public', 'us-cities.json'));
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

// Live activity: group recent leads (last N minutes) by node + industry + city
// so we can show what each worker is currently scraping (for local scrapes that
// don't go through the fleet-job dispatcher).
app.get('/api/admin/activity', requireAdmin, (req, res) => {
  const windowSec = Math.min(+req.query.window || 180, 3600); // default 3 min
  const cutoff = now() - windowSec;
  const rows = db.prepare(`
    SELECT l.node_id, l.industry, l.city, l.state,
           COUNT(*) AS leads_count,
           MIN(l.created_at) AS first_at,
           MAX(l.created_at) AS last_at,
           n.label AS node_label, n.hostname AS node_hostname, n.status AS node_status
    FROM leads l
    LEFT JOIN nodes n ON n.id = l.node_id
    WHERE l.created_at >= ? AND l.job_id IS NULL
    GROUP BY l.node_id, l.industry, l.city
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

// ==================== STATIC ====================
app.use(express.static(path.join(__dirname, 'public')));

app.get('/healthz', (req, res) => res.json({ ok: true, t: now() }));

app.listen(PORT, () => {
  console.log(`[fleet] listening on :${PORT}`);
  console.log(`[fleet] license gate: ${LICENSE_HASHES.length ? `${LICENSE_HASHES.length} hashes` : 'OPEN (set LICENSE_HASHES)'}`);
});
