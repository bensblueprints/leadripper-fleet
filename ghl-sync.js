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
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;
const AUTO_SYNC_BATCH = 100;
const AUTO_SYNC_IDLE_MS = 30_000;
const AUTO_SYNC_ERROR_MS = 60_000;
const AUTO_SYNC_MAX_CONSECUTIVE_ERRORS = 10;

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
  trigger: '', // 'all' | 'selected' | 'filter' | 'auto'
};

let autoSyncTimer = null;
let consecutiveErrors = 0;

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

function buildContactPayload(lead, locationId, forUpdate = false) {
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
  if (!forUpdate) p.locationId = locationId;
  if (lead.email && String(lead.email).includes('@')) p.email = lead.email;
  return p;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function isDupeError(body) {
  if (!body) return false;
  const msg = String(body.message || body.error || body.msg || JSON.stringify(body)).toLowerCase();
  return msg.includes('duplicate') || msg.includes('duplicated') || msg.includes('already exists');
}

async function withRetry(fn, label) {
  for (let i = 0; i < MAX_RETRIES; i++) {
    try {
      const r = await fn();
      return r;
    } catch (e) {
      const isTransient = e.message.includes('network') || e.message.includes('timeout') || e.message.includes('ECONNRESET') || e.message.includes('ETIMEDOUT');
      if (!isTransient && i === 0) throw e; // non-transient errors fail immediately
      if (i === MAX_RETRIES - 1) throw e;
      console.log(`[ghl-sync] retry ${label} (${i + 1}/${MAX_RETRIES}): ${e.message}`);
      await sleep(RETRY_DELAY_MS * (i + 1));
    }
  }
}

async function lookupByPhone(apiKey, locationId, phone) {
  const digitsOnly = String(phone).replace(/\D/g, '');
  const phonesToTry = [phone];
  if (!String(phone).startsWith('+1') && digitsOnly.length === 10) {
    phonesToTry.push('+1' + digitsOnly);
    phonesToTry.push('1' + digitsOnly);
  }
  if (digitsOnly.length === 11 && digitsOnly.startsWith('1')) {
    phonesToTry.push('+1' + digitsOnly.slice(1));
  }

  for (const ph of [...new Set(phonesToTry)]) {
    const result = await withRetry(async () => {
      const url = `${GHL_BASE}/contacts/lookup?locationId=${encodeURIComponent(locationId)}&phone=${encodeURIComponent(ph)}`;
      const r = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}`, Version: GHL_VERSION } });
      if (r.status === 404) return null;
      if (!r.ok) throw new Error(`lookup HTTP ${r.status}`);
      const d = await r.json().catch(() => ({}));
      return d?.contacts?.[0]?.id || null;
    }, 'lookup');
    if (result) return result;
  }

  // Fallback: broad search by last 10 digits
  if (digitsOnly.length >= 10) {
    const q = digitsOnly.slice(-10);
    const searchResult = await withRetry(async () => {
      const url = `${GHL_BASE}/contacts/?locationId=${encodeURIComponent(locationId)}&query=${encodeURIComponent(q)}&limit=5`;
      const r = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}`, Version: GHL_VERSION } });
      if (!r.ok) throw new Error(`search HTTP ${r.status}`);
      const d = await r.json().catch(() => ({}));
      const contacts = d?.contacts || [];
      // Match by digits overlap
      for (const c of contacts) {
        const cPhone = String(c.phone || c.cellPhone || c.businessPhone || '').replace(/\D/g, '');
        if (cPhone.slice(-10) === q) return c.id;
      }
      return null;
    }, 'search');
    if (searchResult) return searchResult;
  }

  return null;
}

async function createContact(apiKey, payload) {
  return withRetry(async () => {
    const r = await fetch(`${GHL_BASE}/contacts/`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json', Version: GHL_VERSION },
      body: JSON.stringify(payload),
    });
    const body = await r.json().catch(() => ({}));
    return { ok: r.ok, status: r.status, body };
  }, 'create');
}

async function updateContact(apiKey, id, payload) {
  return withRetry(async () => {
    const r = await fetch(`${GHL_BASE}/contacts/${id}`, {
      method: 'PUT',
      headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json', Version: GHL_VERSION },
      body: JSON.stringify(payload),
    });
    const body = await r.json().catch(() => ({}));
    return { ok: r.ok, status: r.status, body };
  }, 'update');
}

function buildWhere(filter, onlyUnsynced) {
  const where = [`phone IS NOT NULL`, `phone != ''`];
  const params = [];
  if (onlyUnsynced) where.push(`(ghl_synced IS NULL OR ghl_synced = 0)`);
  if (filter?.industry) { where.push(`industry = ?`); params.push(filter.industry); }
  if (filter?.state)    { where.push(`state = ?`);    params.push(filter.state); }
  if (filter?.city)     { where.push(`city = ?`);     params.push(filter.city); }
  if (filter?.tags) {
    // tags column is JSON array text; use LIKE for simple tag matching
    where.push(`tags LIKE ?`);
    params.push(`%"${filter.tags.replace(/"/g, '')}"%`);
  }
  if (filter?.tagsAll && Array.isArray(filter.tagsAll)) {
    for (const t of filter.tagsAll) {
      where.push(`tags LIKE ?`);
      params.push(`%"${String(t).replace(/"/g, '')}"%`);
    }
  }
  if (filter?.q) {
    where.push(`(name LIKE ? OR phone LIKE ? OR email LIKE ? OR website LIKE ? OR address LIKE ?)`);
    const q = `%${filter.q}%`;
    params.push(q, q, q, q, q);
  }
  return { whereClause: where.join(' AND '), params };
}

function countLeads(db, filter, onlyUnsynced) {
  const { whereClause, params } = buildWhere(filter, onlyUnsynced);
  const row = db.prepare(`SELECT COUNT(*) AS c FROM leads WHERE ${whereClause}`).get(...params);
  return row?.c || 0;
}

function pickLeadsBatch(db, { filter, onlyUnsynced = true, offset = 0, batchSize = 1000 }) {
  const { whereClause, params } = buildWhere(filter, onlyUnsynced);
  return db.prepare(
    `SELECT * FROM leads WHERE ${whereClause} ORDER BY id DESC LIMIT ? OFFSET ?`
  ).all(...params, batchSize, offset);
}

function pickLeads(db, { leadIds, filter, onlyUnsynced = true }) {
  if (Array.isArray(leadIds) && leadIds.length) {
    const placeholders = leadIds.map(() => '?').join(',');
    return db.prepare(
      `SELECT * FROM leads WHERE id IN (${placeholders}) AND phone IS NOT NULL AND phone != ''`
    ).all(...leadIds);
  }
  // For single-batch queries (backward compat), return first batch only
  return pickLeadsBatch(db, { filter, onlyUnsynced, offset: 0, batchSize: 1000 });
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

  const totalLeads = countLeads(db, opts.filter, opts.onlyUnsynced !== false);
  if (!totalLeads) {
    appendLog(`no leads match — nothing to sync`);
    return { ok: true, total: 0 };
  }

  Object.assign(state, {
    running: true, cancel: false,
    started_at: Math.floor(Date.now() / 1000), finished_at: 0,
    total: totalLeads, done: 0, created: 0, updated: 0, errors: 0, skipped: 0,
    current: '', last_error: '', last_log: [],
    trigger: opts.trigger || 'manual',
  });
  appendLog(`starting sync of ${totalLeads} leads in batches of 1000 (trigger=${state.trigger})`);

  (async () => {
    const BATCH_SIZE = 1000;
    let offset = 0;
    while (offset < totalLeads) {
      if (state.cancel) { appendLog('cancelled by admin'); break; }
      const batch = pickLeadsBatch(db, { filter: opts.filter, onlyUnsynced: opts.onlyUnsynced !== false, offset, batchSize: BATCH_SIZE });
      if (!batch.length) break;

      for (const lead of batch) {
        if (state.cancel) { appendLog('cancelled by admin'); break; }
        state.current = lead.name || lead.phone || ('#' + lead.id);

        // Skip leads that already have a known GHL contact ID — they were already synced
        if (lead.ghl_contact_id && String(lead.ghl_contact_id).trim()) {
          state.skipped++;
          state.done++;
          appendLog(`skipped (already has ghl_contact_id): ${state.current}`);
          await new Promise(r => setTimeout(r, GAP_MS));
          continue;
        }

        try {
          const createPayload = buildContactPayload(lead, locationId);
          const updatePayload = buildContactPayload(lead, locationId, true);
          let existingId = null;
          try { existingId = await lookupByPhone(apiKey, locationId, lead.phone); } catch {}
          if (existingId) {
            const r = await updateContact(apiKey, existingId, updatePayload);
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
            const r = await createContact(apiKey, createPayload);
            if (r.ok) {
              state.created++;
              markSynced(db, lead.id, r.body?.contact?.id || r.body?.id || '');
              appendLog(`created: ${state.current}`);
            } else if (r.status === 422 || isDupeError(r.body)) {
              // "duplicate contact" — try to extract existing contact ID from response,
              // or fall back to a broader phone lookup now that we try multiple formats.
              let dupeId = r.body?.meta?.contactId || r.body?.contact?.id || r.body?.id || r.body?.data?.id || '';
              if (!dupeId) {
                try { dupeId = await lookupByPhone(apiKey, locationId, lead.phone); } catch {}
              }
              if (dupeId) {
                // Update the existing contact so it has latest info + tags
                try {
                  const ur = await updateContact(apiKey, dupeId, updatePayload);
                  if (ur.ok) {
                    state.updated++;
                    markSynced(db, lead.id, dupeId);
                    appendLog(`updated dupe: ${state.current} (id=${dupeId})`);
                  } else {
                    state.errors++;
                    state.last_error = ur.body?.message || ('HTTP ' + ur.status);
                    appendLog(`error updating dupe ${state.current}: ${state.last_error}`);
                  }
                } catch (e) {
                  state.errors++;
                  appendLog(`exception updating dupe ${state.current}: ${e.message}`);
                }
              } else {
                // Can't find the dupe — mark synced so we don't retry forever
                state.updated++;
                markSynced(db, lead.id, '');
                appendLog(`dupe not found, marked synced: ${state.current}`);
              }
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
      offset += BATCH_SIZE;
      if (!state.cancel && offset < totalLeads) {
        appendLog(`batch complete — ${state.done} / ${totalLeads} processed`);
      }
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

  return { ok: true, total: totalLeads };
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

// ---------- continuous background sync ----------
// Runs indefinitely: pulls unsynced leads in small batches, syncs them, sleeps
// when caught up. Resumes automatically on restart because unsynced leads are
// simply re-queried — no cursor needed.
async function autoSync(db) {
  const { apiKey, locationId } = getCreds(db);
  if (!apiKey || !locationId) {
    appendLog('auto-sync skipped: no GHL credentials');
    return;
  }

  // Load last cursor (highest id already processed in auto mode) so we don't
  // re-scan already-synced leads from previous sessions.
  let cursor = 0;
  try {
    const row = db.prepare(`SELECT value FROM settings WHERE key = 'ghl_auto_sync_cursor'`).get();
    if (row) cursor = +row.value || 0;
  } catch {}

  while (true) {
    if (state.cancel) { appendLog('auto-sync cancelled'); break; }

    try {
      const batch = db.prepare(`
        SELECT id, name, phone, email, website, address, city, state, industry,
               website_platform, website_status, tags, ghl_contact_id
        FROM leads
        WHERE ghl_synced = 0 AND phone IS NOT NULL AND phone != '' AND id > ?
        ORDER BY id ASC LIMIT ?
      `).all(cursor, AUTO_SYNC_BATCH);

      if (!batch.length) {
        // Caught up — reset cursor to 0 so new leads are picked up next cycle
        cursor = 0;
        db.prepare(`INSERT OR REPLACE INTO settings (key, value) VALUES ('ghl_auto_sync_cursor', '0')`).run();
        appendLog('auto-sync caught up — sleeping 30s');
        await sleep(AUTO_SYNC_IDLE_MS);
        continue;
      }

      for (const lead of batch) {
        if (state.cancel) break;
        cursor = Math.max(cursor, lead.id);
        state.current = lead.name || lead.phone || ('#' + lead.id);

        if (lead.ghl_contact_id && String(lead.ghl_contact_id).trim()) {
          state.skipped++;
          continue;
        }

        try {
          const createPayload = buildContactPayload(lead, locationId);
          const updatePayload = buildContactPayload(lead, locationId, true);
          let existingId = null;
          try { existingId = await lookupByPhone(apiKey, locationId, lead.phone); } catch {}
          if (existingId) {
            const r = await updateContact(apiKey, existingId, updatePayload);
            if (r.ok) {
              state.updated++;
              markSynced(db, lead.id, existingId);
              consecutiveErrors = 0;
            } else {
              state.errors++; consecutiveErrors++;
              state.last_error = r.body?.message || ('HTTP ' + r.status);
            }
          } else {
            const r = await createContact(apiKey, createPayload);
            if (r.ok) {
              state.created++;
              markSynced(db, lead.id, r.body?.contact?.id || r.body?.id || '');
              consecutiveErrors = 0;
            } else if (r.status === 422 || isDupeError(r.body)) {
              let dupeId = r.body?.meta?.contactId || r.body?.contact?.id || r.body?.id || r.body?.data?.id || '';
              if (!dupeId) {
                try { dupeId = await lookupByPhone(apiKey, locationId, lead.phone); } catch {}
              }
              if (dupeId) {
                try {
                  const ur = await updateContact(apiKey, dupeId, updatePayload);
                  if (ur.ok) {
                    state.updated++;
                    markSynced(db, lead.id, dupeId);
                    appendLog(`auto updated dupe: ${state.current}`);
                  } else {
                    state.errors++; consecutiveErrors++;
                    state.last_error = ur.body?.message || ('HTTP ' + ur.status);
                  }
                } catch (e) {
                  state.errors++; consecutiveErrors++;
                  state.last_error = e.message;
                }
              } else {
                state.updated++;
                markSynced(db, lead.id, '');
                appendLog(`auto dupe not found, marked synced: ${state.current}`);
              }
              consecutiveErrors = 0;
            } else {
              state.errors++; consecutiveErrors++;
              state.last_error = r.body?.message || ('HTTP ' + r.status);
            }
          }
        } catch (e) {
          state.errors++; consecutiveErrors++;
          state.last_error = e.message;
        }
        state.done++;
        await sleep(GAP_MS);
      }

      // Persist cursor so a restart resumes near this point
      db.prepare(`INSERT OR REPLACE INTO settings (key, value) VALUES ('ghl_auto_sync_cursor', ?)`).run(String(cursor));

      if (consecutiveErrors >= AUTO_SYNC_MAX_CONSECUTIVE_ERRORS) {
        appendLog(`auto-sync paused: ${consecutiveErrors} consecutive errors (${state.last_error})`);
        await sleep(AUTO_SYNC_ERROR_MS);
        consecutiveErrors = 0;
      }
    } catch (e) {
      appendLog('auto-sync loop error: ' + e.message);
      await sleep(AUTO_SYNC_ERROR_MS);
    }
  }
}

function startAutoSync(db) {
  if (autoSyncTimer) return; // already running
  state.cancel = false;
  appendLog('auto-sync starting');
  autoSyncTimer = autoSync(db).catch(e => {
    appendLog('auto-sync crashed: ' + e.message);
  }).finally(() => {
    autoSyncTimer = null;
    // Auto-restart after error
    setTimeout(() => startAutoSync(db), AUTO_SYNC_ERROR_MS);
  });
}

function stopAutoSync() {
  state.cancel = true;
  if (autoSyncTimer) {
    // Can't truly stop the promise, but cancel flag will exit the loop
    autoSyncTimer = null;
  }
}

module.exports = { runSync, cancel, status, stats, getCreds, saveCreds, bulkResetSync, startAutoSync, stopAutoSync };
