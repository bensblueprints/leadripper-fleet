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
    INSERT INTO leads (job_id, node_id, name, phone, email, website, address, city, state,
      industry, gcid, search_term, google_category_raw, rating, reviews, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const txn = db.transaction(() => {
    for (const l of leads) {
      insertLead.run(job.id, node.id,
        l.name || null, l.phone || null, l.email || null, l.website || null,
        l.address || null, l.city || job.city, l.state || job.state,
        l.industry || null, l.gcid || null, l.search_term || job.industry,
        l.google_category_raw || null, l.rating || null, l.reviews || null, t);
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

// Dispatch a job. If cities is an array or "all", fans out to multiple jobs.
app.post('/api/admin/dispatch', requireAdmin, (req, res) => {
  const { industry, cities = [], state, target_node_id = null, priority = 0 } = req.body || {};
  if (!industry || !cities.length) return res.status(400).json({ error: 'industry + cities required' });

  const ins = db.prepare(`
    INSERT INTO jobs (industry, city, state, target_node_id, priority, status)
    VALUES (?, ?, ?, ?, ?, 'queued')
  `);
  let count = 0;
  const txn = db.transaction(() => {
    for (const city of cities) {
      ins.run(industry, city, state || null, target_node_id || null, priority);
      count++;
    }
  });
  txn();
  res.json({ ok: true, dispatched: count });
});

app.post('/api/admin/jobs/:id/cancel', requireAdmin, (req, res) => {
  const id = +req.params.id;
  const job = db.prepare('SELECT * FROM jobs WHERE id=?').get(id);
  if (!job) return res.status(404).json({ error: 'not found' });
  if (job.status === 'running' && job.assigned_node_id) {
    db.prepare(`INSERT INTO commands (node_id, kind, payload) VALUES (?, 'cancel_job', ?)`)
      .run(job.assigned_node_id, JSON.stringify({ job_id: id }));
  }
  db.prepare(`UPDATE jobs SET status='cancelled', finished_at=? WHERE id=?`).run(now(), id);
  res.json({ ok: true });
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
