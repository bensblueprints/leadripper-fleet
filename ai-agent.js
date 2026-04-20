// Fleet AI assistant — chat interface that can query leads, kick off GHL syncs,
// and control mass-scrape. Tool-calling via Anthropic Messages API.
// Uses the API key saved in settings (anthropic_api_key).

const ghlSync = require('./ghl-sync');

const SYSTEM_PROMPT = `You are the LeadRipper Fleet admin assistant. You help the operator query the master lead database, build targeted lead lists, and kick off GoHighLevel syncs.

You have access to tools for listing/counting/updating leads, starting GHL syncs, and controlling the mass-scrape orchestrator. Prefer count_leads before list_leads when the user's query might match a huge set. When building a list the user asked for, return a concise summary plus the key filters you'd use — don't dump hundreds of rows unless asked.

When the user asks you to "send these to GHL" or "sync X", use sync_ghl with the appropriate filter or leadIds. Always confirm the scope (e.g. "syncing 438 unsynced plumbers in FL — proceed?") before calling destructive tools unless the user has already said "do it" or equivalent.

Keep replies short and practical. Surface numbers (counts, phase progress) rather than prose.`;

const TOOLS = [
  {
    name: 'list_leads',
    description: 'Query the master leads table with filters. Returns matching rows (up to limit).',
    input_schema: {
      type: 'object',
      properties: {
        q: { type: 'string', description: 'free-text search across name/phone/email/website/address' },
        industry: { type: 'string' },
        state: { type: 'string', description: '2-letter state code' },
        city: { type: 'string' },
        hasPhone: { type: 'boolean' },
        ghlSynced: { type: 'boolean', description: 'true = only synced, false = only unsynced' },
        limit: { type: 'integer', description: 'default 50, max 500' },
        offset: { type: 'integer' },
      },
    },
  },
  {
    name: 'count_leads',
    description: 'Count leads matching filters (cheaper than list for scoping questions).',
    input_schema: {
      type: 'object',
      properties: {
        q: { type: 'string' },
        industry: { type: 'string' },
        state: { type: 'string' },
        city: { type: 'string' },
        hasPhone: { type: 'boolean' },
        ghlSynced: { type: 'boolean' },
      },
    },
  },
  {
    name: 'list_distinct',
    description: 'List distinct values for a column (industry, state, city, website_platform).',
    input_schema: {
      type: 'object',
      required: ['field'],
      properties: {
        field: { type: 'string', enum: ['industry','state','city','website_platform','website_status'] },
        limit: { type: 'integer', description: 'default 50' },
      },
    },
  },
  {
    name: 'update_lead_tags',
    description: 'Set tags (array of strings) on a specific lead.',
    input_schema: {
      type: 'object',
      required: ['id','tags'],
      properties: {
        id: { type: 'integer' },
        tags: { type: 'array', items: { type: 'string' } },
      },
    },
  },
  {
    name: 'sync_ghl',
    description: 'Kick off a GoHighLevel sync. Pass leadIds for a specific set, or filter to select by industry/state/city/etc. Runs in the background on the server; returns total queued.',
    input_schema: {
      type: 'object',
      properties: {
        leadIds: { type: 'array', items: { type: 'integer' } },
        filter: {
          type: 'object',
          properties: {
            q: { type: 'string' },
            industry: { type: 'string' },
            state: { type: 'string' },
            city: { type: 'string' },
            limit: { type: 'integer' },
          },
        },
        onlyUnsynced: { type: 'boolean', description: 'default true — skip already-synced leads' },
      },
    },
  },
  {
    name: 'ghl_status',
    description: 'Get current GHL sync status: running flag, progress, created/updated/errors counts, recent log lines.',
    input_schema: { type: 'object', properties: {} },
  },
  {
    name: 'cancel_ghl_sync',
    description: 'Cancel an in-progress GHL sync.',
    input_schema: { type: 'object', properties: {} },
  },
  {
    name: 'mass_scrape_control',
    description: 'Control the mass-scrape orchestrator: start/pause the dispatcher, retry failed coverage rows, or seed phase 2 (remaining cities).',
    input_schema: {
      type: 'object',
      required: ['action'],
      properties: {
        action: { type: 'string', enum: ['start','pause','retry-failed','seed-phase2','status'] },
      },
    },
  },
  {
    name: 'fleet_stats',
    description: 'Get fleet-wide stats: node counts, job counts, leads in last 24h / total.',
    input_schema: { type: 'object', properties: {} },
  },
];

// ---------- Tool executors ----------

function clampInt(v, min, max, d) {
  const n = +v;
  if (!Number.isFinite(n)) return d;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

function buildWhere(args) {
  const where = [];
  const params = [];
  if (args.q) {
    where.push(`(name LIKE ? OR phone LIKE ? OR email LIKE ? OR website LIKE ? OR address LIKE ?)`);
    const q = `%${args.q}%`;
    params.push(q, q, q, q, q);
  }
  if (args.industry) { where.push('industry = ?'); params.push(args.industry); }
  if (args.state)    { where.push('state = ?');    params.push(args.state); }
  if (args.city)     { where.push('city LIKE ?');  params.push(`%${args.city}%`); }
  if (args.hasPhone === true)  where.push(`phone IS NOT NULL AND phone != ''`);
  if (args.hasPhone === false) where.push(`(phone IS NULL OR phone = '')`);
  if (args.ghlSynced === true)  where.push(`ghl_synced = 1`);
  if (args.ghlSynced === false) where.push(`(ghl_synced IS NULL OR ghl_synced = 0)`);
  return { where, params };
}

function tool_list_leads(db, args) {
  const { where, params } = buildWhere(args);
  const limit = clampInt(args.limit, 1, 500, 50);
  const offset = clampInt(args.offset, 0, 1e9, 0);
  const sql = `SELECT id, name, phone, email, website, city, state, industry, rating, reviews, website_status, website_platform, ghl_synced
               FROM leads ${where.length ? 'WHERE '+where.join(' AND ') : ''}
               ORDER BY id DESC LIMIT ? OFFSET ?`;
  const rows = db.prepare(sql).all(...params, limit, offset);
  const total = db.prepare(`SELECT COUNT(*) AS c FROM leads ${where.length ? 'WHERE '+where.join(' AND ') : ''}`).get(...params).c;
  return { total, returned: rows.length, limit, offset, rows };
}

function tool_count_leads(db, args) {
  const { where, params } = buildWhere(args);
  const c = db.prepare(`SELECT COUNT(*) AS c FROM leads ${where.length ? 'WHERE '+where.join(' AND ') : ''}`).get(...params).c;
  return { count: c };
}

function tool_list_distinct(db, args) {
  const allowed = ['industry','state','city','website_platform','website_status'];
  if (!allowed.includes(args.field)) return { error: 'invalid field' };
  const limit = clampInt(args.limit, 1, 2000, 50);
  const rows = db.prepare(
    `SELECT ${args.field} AS value, COUNT(*) AS n FROM leads WHERE ${args.field} IS NOT NULL AND ${args.field} != '' GROUP BY ${args.field} ORDER BY n DESC LIMIT ?`
  ).all(limit);
  return { values: rows };
}

function tool_update_lead_tags(db, args) {
  const id = +args.id;
  if (!id) return { error: 'id required' };
  const tags = Array.isArray(args.tags) ? args.tags.map(String) : [];
  const r = db.prepare(`UPDATE leads SET tags = ? WHERE id = ?`).run(JSON.stringify(tags), id);
  return { ok: true, changed: r.changes, id, tags };
}

async function tool_sync_ghl(db, args) {
  try {
    const r = await ghlSync.runSync(db, {
      leadIds: Array.isArray(args.leadIds) ? args.leadIds.map(Number).filter(Boolean) : null,
      filter: args.filter || null,
      onlyUnsynced: args.onlyUnsynced !== false,
      trigger: 'ai-agent',
    });
    return r;
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

function tool_ghl_status(db) {
  return { status: ghlSync.status(), stats: ghlSync.stats(db) };
}

function tool_cancel_ghl_sync() {
  ghlSync.cancel();
  return { ok: true };
}

function tool_mass_scrape_control(db, args) {
  const a = args.action;
  if (a === 'start') {
    db.prepare(`INSERT OR REPLACE INTO settings (key, value) VALUES ('mass_scrape_enabled', '1')`).run();
    return { ok: true, enabled: true };
  }
  if (a === 'pause') {
    db.prepare(`INSERT OR REPLACE INTO settings (key, value) VALUES ('mass_scrape_enabled', '0')`).run();
    return { ok: true, enabled: false };
  }
  if (a === 'retry-failed') {
    const n = db.prepare(`UPDATE scrape_coverage SET status='pending', last_error=NULL WHERE status='failed'`).run().changes;
    return { ok: true, requeued: n };
  }
  if (a === 'seed-phase2') {
    const massSeed = require('./mass-scrape/seed');
    return massSeed.seedPhase2(db, { logger: () => {} });
  }
  if (a === 'status') {
    const massScheduler = require('./mass-scrape/scheduler');
    return massScheduler.stats(db);
  }
  return { error: 'unknown action' };
}

function tool_fleet_stats(db) {
  const nodesCount = db.prepare(`SELECT status, COUNT(*) AS c FROM nodes GROUP BY status`).all();
  const jobsCount  = db.prepare(`SELECT status, COUNT(*) AS c FROM jobs GROUP BY status`).all();
  const now = Math.floor(Date.now() / 1000);
  const leads24h = db.prepare(`SELECT COUNT(*) AS c FROM leads WHERE created_at > ?`).get(now - 86400).c;
  const leadsTotal = db.prepare(`SELECT COUNT(*) AS c FROM leads`).get().c;
  return { nodes: nodesCount, jobs: jobsCount, leads_24h: leads24h, leads_total: leadsTotal };
}

async function runTool(db, name, args) {
  try {
    switch (name) {
      case 'list_leads':           return tool_list_leads(db, args);
      case 'count_leads':          return tool_count_leads(db, args);
      case 'list_distinct':        return tool_list_distinct(db, args);
      case 'update_lead_tags':     return tool_update_lead_tags(db, args);
      case 'sync_ghl':             return await tool_sync_ghl(db, args);
      case 'ghl_status':           return tool_ghl_status(db);
      case 'cancel_ghl_sync':      return tool_cancel_ghl_sync();
      case 'mass_scrape_control':  return tool_mass_scrape_control(db, args);
      case 'fleet_stats':          return tool_fleet_stats(db);
      default: return { error: 'unknown tool: ' + name };
    }
  } catch (e) {
    return { error: e.message };
  }
}

// ---------- Provider: Anthropic ----------

function getProviderConfig(db) {
  const rows = db.prepare(`SELECT key, value FROM settings WHERE key IN ('ai_provider','anthropic_api_key','openai_api_key','groq_api_key','xai_api_key')`).all();
  const m = Object.fromEntries(rows.map(r => [r.key, r.value]));
  return {
    provider: m.ai_provider || 'anthropic',
    anthropic: m.anthropic_api_key || '',
    openai:    m.openai_api_key    || '',
    groq:      m.groq_api_key      || '',
    xai:       m.xai_api_key       || '',
  };
}

async function callAnthropic(apiKey, messages) {
  const r = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-5-20250929',
      max_tokens: 4096,
      system: SYSTEM_PROMPT,
      tools: TOOLS,
      messages,
    }),
  });
  const data = await r.json();
  if (!r.ok) throw new Error(data?.error?.message || ('HTTP ' + r.status));
  return data;
}

// Convert OpenAI-style chat history coming from client into Anthropic `messages` format,
// preserving prior tool_use/tool_result blocks.
function toAnthropicMessages(history) {
  // history entries: {role: 'user'|'assistant', content: string} OR
  //                  {role: 'assistant', content: [{type:'text',...},{type:'tool_use',...}]} OR
  //                  {role: 'user', content: [{type:'tool_result',...}]}
  return history.map(m => {
    if (Array.isArray(m.content)) return { role: m.role, content: m.content };
    return { role: m.role, content: [{ type: 'text', text: String(m.content || '') }] };
  });
}

async function chat(db, { history, userMessage, maxHops = 6 }) {
  const cfg = getProviderConfig(db);
  if (cfg.provider !== 'anthropic') {
    throw new Error(`AI chat currently supports provider=anthropic only (got "${cfg.provider}"). Change it on the Mass Scrape tab.`);
  }
  if (!cfg.anthropic) throw new Error('Anthropic API key not configured. Save one on the Mass Scrape → AI Provider panel.');

  const msgs = toAnthropicMessages(history || []);
  if (userMessage) msgs.push({ role: 'user', content: [{ type: 'text', text: String(userMessage) }] });

  let hops = 0;
  const transcript = [...msgs]; // we'll mutate/append

  while (hops++ < maxHops) {
    const resp = await callAnthropic(cfg.anthropic, transcript);
    // Append the assistant turn
    transcript.push({ role: 'assistant', content: resp.content });

    // Collect any tool_use blocks
    const toolUses = (resp.content || []).filter(b => b.type === 'tool_use');
    if (!toolUses.length) {
      // Plain text reply — done
      return { messages: transcript.slice(msgs.length - (userMessage ? 1 : 0)), stop_reason: resp.stop_reason };
    }

    // Execute tools and send results back
    const toolResults = [];
    for (const tu of toolUses) {
      const result = await runTool(db, tu.name, tu.input || {});
      toolResults.push({
        type: 'tool_result',
        tool_use_id: tu.id,
        content: JSON.stringify(result).slice(0, 50000), // truncate giant results
      });
    }
    transcript.push({ role: 'user', content: toolResults });
  }
  return { messages: transcript.slice(msgs.length - (userMessage ? 1 : 0)), stop_reason: 'max_hops' };
}

module.exports = { chat, TOOLS };
