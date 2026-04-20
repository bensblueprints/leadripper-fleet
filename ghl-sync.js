// Fleet-side GoHighLevel sync.
// Server-side background task: reads leads from master DB, pushes to GHL,
// marks leads as synced. Runs independently of any admin browser tab.
//
// Credentials live in the `settings` table:
//   ghl_api_key         - Private Integration Token (pit-xxxx...)
//   ghl_location_id     - GHL sub-account location id
//
// Single in-memory task state; one run at a time.

const GHL_BASE = 'https://services.leadconnectorhq.com';
const GHL_VERSION = '2021-07-28';
const GAP_MS = 250; // ~4 req/sec, GHL allows 10

const state = {
  running: false,
  cancel: false,
  started_at: 0,
  finished_at: 0,
  total: 0,
  done: 0,
  created: 0,
  updated: 0,
  errors: 0,
  skipped: 0,
  current: '',
  last_error: '',
  last_log: [],
  trigger: '', // 'all' | 'selected' | 'filter'
};

function appendLog(line) {
  state.last_log.push(`[${new Date().toISOString().slice(11,19)}] ${line}`);
  if (state.last_log.length > 200) state.last_log.shift();
}

function getCreds(db) {
  const rows = db.prepare(`SELECT key, value FROM settings WHERE key IN ('ghl_api_key','ghl_location_id')`).all();
  const m = Object.fromEntries(rows.map(r => [r.key, r.value]));
  return { apiKey: m.ghl_api_key || '', locationId: m.ghl_location_id || '' };
}

function saveCreds(db, { apiKey, locationId }) {
  const ins = db.prepare(`INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)`);
  if (typeof apiKey === 'string') ins.run('ghl_api_key', apiKey.trim());
  if (typeof locationId === 'string') ins.run('ghl_location_id', locationId.trim());
}

function buildContactPayload(lead, locationId) {
  const tags = ['leadripper'];
  if (lead.industry) tags.push(String(lead.industry).toLowerCase().replace(/\s+/g, '-'));
  if (lead.website) tags.push('has-website'); else tags.push('no-website');
  if (lead.website_platform) tags.push(String(lead.website_platform).toLowerCase());
  if (lead.website_status === 'broken') tags.push('broken-website');
  if (lead.website_status === 'hacked') tags.push('hacked-website');
  try {
    const lt = JSON.parse(lead.tags || '[]');
    for (const t of lt) if (!tags.includes(t)) tags.push(t);
  } catch {}

  const nameRaw = lead.name || 'Business Owner';
  const parts = String(nameRaw).split(' ');
  const firstName = parts[0] || 'Business';
  const lastName = parts.slice(1).join(' ') || 'Owner';

  const p = {
    locationId,
    firstName,
    lastName,
    companyName: lead.name || '',
    phone: lead.phone,
    address1: lead.address || '',
    city: lead.city || '',
    state: lead.state || '',
    website: lead.website || '',
    source: 'LeadRipper Fleet',
    tags,
  };
  if (lead.email && String(lead.email).includes('@')) p.email = lead.email;
  return p;
}

async function lookupByPhone(apiKey, locationId, phone) {
  const url = `${GHL_BASE}/contacts/lookup?locationId=${encodeURIComponent(locationId)}&phone=${encodeURIComponent(phone)}`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}`, Version: GHL_VERSION } });
  if (!r.ok) return null;
  const d = await r.json().catch(() => ({}));
  return d?.contacts?.[0]?.id || null;
}

async function createContact(apiKey, payload) {
  const r = await fetch(`${GHL_BASE}/contacts/`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json', Version: GHL_VERSION },
    body: JSON.stringify(payload),
  });
  const body = await r.json().catch(() => ({}));
  return { ok: r.ok, status: r.status, body };
}

async function updateContact(apiKey, id, payload) {
  const r = await fetch(`${GHL_BASE}/contacts/${id}`, {
    method: 'PUT',
    headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json', Version: GHL_VERSION },
    body: JSON.stringify(payload),
  });
  const body = await r.json().catch(() => ({}));
  return { ok: r.ok, status: r.status, body };
}

function pickLeads(db, { leadIds, filter, onlyUnsynced = true }) {
  if (Array.isArray(leadIds) && leadIds.length) {
    const placeholders = leadIds.map(() => '?').join(',');
    return db.prepare(
      `SELECT * FROM leads WHERE id IN (${placeholders}) AND phone IS NOT NULL AND phone != ''`
    ).all(...leadIds);
  }
  const where = [`phone IS NOT NULL`, `phone != ''`];
  const params = [];
  if (onlyUnsynced) where.push(`(ghl_synced IS NULL OR ghl_synced = 0)`);
  if (filter?.industry) { where.push(`industry = ?`); params.push(filter.industry); }
  if (filter?.state)    { where.push(`state = ?`);    params.push(filter.state); }
  if (filter?.city)     { where.push(`city = ?`);     params.push(filter.city); }
  if (filter?.q) {
    where.push(`(name LIKE ? OR phone LIKE ? OR email LIKE ? OR website LIKE ? OR address LIKE ?)`);
    const q = `%${filter.q}%`;
    params.push(q, q, q, q, q);
  }
  const limit = Math.min(+filter?.limit || 100000, 1000000);
  return db.prepare(
    `SELECT * FROM leads WHERE ${where.join(' AND ')} ORDER BY id DESC LIMIT ?`
  ).all(...params, limit);
}

function markSynced(db, leadId, contactId) {
  db.prepare(
    `UPDATE leads SET ghl_synced = 1, ghl_contact_id = ?, ghl_synced_at = ? WHERE id = ?`
  ).run(contactId || '', Math.floor(Date.now() / 1000), leadId);
}

async function runSync(db, opts = {}) {
  if (state.running) throw new Error('sync already running');
  const { apiKey, locationId } = getCreds(db);
  if (!apiKey || !locationId) throw new Error('GHL credentials not configured');

  const leads = pickLeads(db, opts);
  if (!leads.length) {
    appendLog(`no leads match — nothing to sync`);
    return { ok: true, total: 0 };
  }

  Object.assign(state, {
    running: true, cancel: false,
    started_at: Math.floor(Date.now() / 1000), finished_at: 0,
    total: leads.length, done: 0, created: 0, updated: 0, errors: 0, skipped: 0,
    current: '', last_error: '', last_log: [],
    trigger: opts.trigger || 'manual',
  });
  appendLog(`starting sync of ${leads.length} leads (trigger=${state.trigger})`);

  (async () => {
    for (const lead of leads) {
      if (state.cancel) { appendLog('cancelled by admin'); break; }
      state.current = lead.name || lead.phone || ('#' + lead.id);
      try {
        const payload = buildContactPayload(lead, locationId);
        let existingId = null;
        try { existingId = await lookupByPhone(apiKey, locationId, lead.phone); } catch {}
        if (existingId) {
          const r = await updateContact(apiKey, existingId, payload);
          if (r.ok) {
            state.updated++;
            markSynced(db, lead.id, existingId);
            appendLog(`updated: ${state.current}`);
          } else {
            state.errors++;
            state.last_error = r.body?.message || ('HTTP ' + r.status);
            appendLog(`error updating ${state.current}: ${state.last_error}`);
          }
        } else {
          const r = await createContact(apiKey, payload);
          if (r.ok) {
            state.created++;
            markSynced(db, lead.id, r.body?.contact?.id || r.body?.id || '');
            appendLog(`created: ${state.current}`);
          } else if (r.status === 422) {
            // "duplicate contact" — mark synced so we skip next time
            state.updated++;
            markSynced(db, lead.id, r.body?.meta?.contactId || '');
            appendLog(`dupe: ${state.current}`);
          } else {
            state.errors++;
            state.last_error = r.body?.message || ('HTTP ' + r.status);
            appendLog(`error creating ${state.current}: ${state.last_error}`);
          }
        }
      } catch (e) {
        state.errors++;
        state.last_error = e.message;
        appendLog(`exception ${state.current}: ${e.message}`);
      }
      state.done++;
      await new Promise(r => setTimeout(r, GAP_MS));
    }
    state.running = false;
    state.finished_at = Math.floor(Date.now() / 1000);
    appendLog(`finished: ${state.created} created, ${state.updated} updated, ${state.errors} errors`);
  })().catch(e => {
    state.running = false;
    state.finished_at = Math.floor(Date.now() / 1000);
    state.last_error = e.message;
    appendLog(`fatal: ${e.message}`);
  });

  return { ok: true, total: leads.length };
}

function cancel() { if (state.running) state.cancel = true; }

function status() {
  return {
    running: state.running,
    trigger: state.trigger,
    started_at: state.started_at,
    finished_at: state.finished_at,
    total: state.total,
    done: state.done,
    created: state.created,
    updated: state.updated,
    errors: state.errors,
    skipped: state.skipped,
    current: state.current,
    last_error: state.last_error,
    log_tail: state.last_log.slice(-40),
  };
}

function stats(db) {
  const row = db.prepare(`
    SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN ghl_synced = 1 THEN 1 ELSE 0 END) AS synced,
      SUM(CASE WHEN (ghl_synced IS NULL OR ghl_synced = 0) AND phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) AS unsynced_with_phone
    FROM leads
  `).get();
  return row || { total: 0, synced: 0, unsynced_with_phone: 0 };
}

function bulkResetSync(db, leadIds) {
  if (!Array.isArray(leadIds) || !leadIds.length) return 0;
  const placeholders = leadIds.map(() => '?').join(',');
  const r = db.prepare(
    `UPDATE leads SET ghl_synced = 0, ghl_contact_id = NULL, ghl_synced_at = NULL WHERE id IN (${placeholders})`
  ).run(...leadIds);
  return r.changes;
}

module.exports = { runSync, cancel, status, stats, getCreds, saveCreds, bulkResetSync };
