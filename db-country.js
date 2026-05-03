const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

const DATA_DIR = process.env.DATA_DIR || '/data';
const COUNTRIES_DIR = path.join(DATA_DIR, 'countries');
if (!fs.existsSync(COUNTRIES_DIR)) fs.mkdirSync(COUNTRIES_DIR, { recursive: true });

// State/province territory mapping
const COUNTRY_MAP = {
  USA: new Set(['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC','PR']),
  CANADA: new Set(['ON','BC','AB','SK','MB','QC','NB','NS','PE','NL','NT','NU','YT']),
  AUSTRALIA: new Set(['NSW','VIC','QLD','WA','SA','TAS','ACT','NT']),
};

// Common UK cities for heuristic detection
const UK_CITIES = new Set([
  'london','manchester','birmingham','leeds','glasgow','sheffield','bradford','liverpool',
  'edinburgh','bristol','cardiff','belfast','leicester','coventry','nottingham','newcastle',
  'sunderland','brighton','hull','plymouth','stoke','wolverhampton','derby','swansea',
  'southampton','portsmouth','york','oxford','cambridge','bath','exeter','norwich',
  'aberdeen','dundee','inverness','baildon','yeadon','guiseley','shipley','ilkley',
  'keighley','brighouse','halifax','huddersfield','wakefield','doncaster','rotherham',
  'barnsley','oldham','stockport','bolton','bury','rochdale','salford','wigan',
  'blackpool','preston','blackburn','burnley','accrington','nelson','colne',
  'skipton','settler','harrogate','ripon','knaresborough','otley','pudsey',
  'morley','batley','dewsbury','castleford','pontefract','normanton','knottingley',
  'featherstone','hemworth','south elmsall','crofton','ryhill','fitzwilliam',
  'ackworth','wintersett','hessle','brough','goole','howden','selby','tadcaster',
  'wetherby','collingham','boston spa','garforth','kippax','swillington',
  'woodlesford','methley','kippax','allerton bywater','castleford','pontefract'
]);

// UK-specific industries
const UK_INDUSTRIES = new Set([
  'gas engineer','heating contractor','plumbing supply store','drainage service',
  'bathroom supply store','kitchen remodeler','damp proofing specialist',
  'loft conversion company','double glazing installer','boiler supplier'
]);

function detectCountry(lead) {
  const state = (lead.state || '').toString().trim().toUpperCase();
  const city = (lead.city || '').toString().trim().toLowerCase();
  const industry = (lead.industry || '').toString().trim().toLowerCase();
  const phone = (lead.phone || '').toString().trim();
  const address = (lead.address || '').toString().trim().toLowerCase();

  // Explicit country field
  if (lead.country && typeof lead.country === 'string') {
    const cc = lead.country.trim().toUpperCase();
    if (cc === 'USA' || cc === 'US' || cc === 'UNITED STATES') return 'USA';
    if (cc === 'UK' || cc === 'GB' || cc === 'UNITED KINGDOM') return 'UK';
    if (cc === 'CA' || cc === 'CANADA') return 'CANADA';
    if (cc === 'AU' || cc === 'AUSTRALIA') return 'AUSTRALIA';
    return cc;
  }

  // Phone prefix detection
  if (phone.startsWith('+1') || phone.startsWith('1-')) return 'USA';
  if (phone.startsWith('+44') || phone.startsWith('44 ')) return 'UK';
  if (phone.startsWith('+1 ') && phone.length > 4) {
    // Could be US or Canada - fall through to state check
  }

  // State/province detection
  if (COUNTRY_MAP.USA.has(state)) return 'USA';
  if (COUNTRY_MAP.CANADA.has(state)) return 'CANADA';
  if (COUNTRY_MAP.AUSTRALIA.has(state)) return 'AUSTRALIA';

  // UK heuristics — require empty/null state AND at least one strong indicator
  const hasUKPostcode = /\b[A-Z]{1,2}\d[A-Z\d]? \d[A-Z]{2}\b/i.test(address);
  const hasUKAddress = address.includes('united kingdom') || address.includes(', uk');
  const hasUKIndustry = UK_INDUSTRIES.has(industry);
  const hasUKName = /\b(ltd|limited|llp)\b/i.test(lead.name || '');
  if (state === '' && (hasUKPostcode || hasUKAddress || hasUKIndustry || hasUKName)) return 'UK';

  // Empty state with no clear indicators → other international
  if (state === '' && city && city.length > 1) {
    return 'OTHER';
  }

  return 'USA'; // Default fallback for ambiguous data
}

const dbCache = {};

function getCountryDb(country) {
  const code = (country || 'OTHER').toUpperCase().replace(/[^A-Z0-9_-]/g, '');
  if (code === 'USA' || code === 'US') return null; // Use main db

  if (!dbCache[code]) {
    const dbPath = path.join(COUNTRIES_DIR, `fleet-${code}.db`);
    const db = new Database(dbPath);
    db.pragma('journal_mode = WAL');
    db.pragma('foreign_keys = ON');

    db.exec(`
      CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        node_id INTEGER,
        name TEXT,
        phone TEXT,
        email TEXT,
        website TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        industry TEXT,
        gcid TEXT,
        search_term TEXT,
        google_category_raw TEXT,
        rating REAL,
        reviews INTEGER,
        website_platform TEXT,
        website_status TEXT DEFAULT 'unchecked',
        tags TEXT DEFAULT '[]',
        created_at INTEGER DEFAULT (strftime('%s','now')),
        synced_at INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads(phone);
      CREATE INDEX IF NOT EXISTS idx_leads_job ON leads(job_id);
      CREATE INDEX IF NOT EXISTS idx_leads_industry ON leads(industry);
      CREATE INDEX IF NOT EXISTS idx_leads_state ON leads(state);
      CREATE INDEX IF NOT EXISTS idx_leads_city ON leads(city);
      CREATE INDEX IF NOT EXISTS idx_leads_name ON leads(name);
    `);

    // Add enrichment columns if missing (migrations)
    try {
      const cols = db.prepare(`PRAGMA table_info(leads)`).all().map(c => c.name);
      if (!cols.includes('business_hours')) db.exec(`ALTER TABLE leads ADD COLUMN business_hours TEXT`);
      if (!cols.includes('reviews_1star')) db.exec(`ALTER TABLE leads ADD COLUMN reviews_1star INTEGER DEFAULT 0`);
      if (!cols.includes('reviews_2star')) db.exec(`ALTER TABLE leads ADD COLUMN reviews_2star INTEGER DEFAULT 0`);
      if (!cols.includes('reviews_3star')) db.exec(`ALTER TABLE leads ADD COLUMN reviews_3star INTEGER DEFAULT 0`);
      if (!cols.includes('reviews_4star')) db.exec(`ALTER TABLE leads ADD COLUMN reviews_4star INTEGER DEFAULT 0`);
      if (!cols.includes('reviews_5star')) db.exec(`ALTER TABLE leads ADD COLUMN reviews_5star INTEGER DEFAULT 0`);
      if (!cols.includes('ai_seo_score')) db.exec(`ALTER TABLE leads ADD COLUMN ai_seo_score INTEGER`);
      if (!cols.includes('ai_design_score')) db.exec(`ALTER TABLE leads ADD COLUMN ai_design_score INTEGER`);
      if (!cols.includes('ai_seo_notes')) db.exec(`ALTER TABLE leads ADD COLUMN ai_seo_notes TEXT`);
      if (!cols.includes('ai_design_notes')) db.exec(`ALTER TABLE leads ADD COLUMN ai_design_notes TEXT`);
      if (!cols.includes('ai_analyzed_at')) db.exec(`ALTER TABLE leads ADD COLUMN ai_analyzed_at INTEGER`);
      if (!cols.includes('ai_provider')) db.exec(`ALTER TABLE leads ADD COLUMN ai_provider TEXT`);
      if (!cols.includes('ghl_synced')) db.exec(`ALTER TABLE leads ADD COLUMN ghl_synced INTEGER DEFAULT 0`);
      if (!cols.includes('ghl_contact_id')) db.exec(`ALTER TABLE leads ADD COLUMN ghl_contact_id TEXT`);
      if (!cols.includes('ghl_synced_at')) db.exec(`ALTER TABLE leads ADD COLUMN ghl_synced_at INTEGER`);
      if (!cols.includes('recat_status')) db.exec(`ALTER TABLE leads ADD COLUMN recat_status TEXT`);
    } catch {}

    try {
      db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS uniq_leads_phone_industry ON leads(phone, industry) WHERE phone IS NOT NULL AND industry IS NOT NULL`);
    } catch {}

    dbCache[code] = db;
  }
  return dbCache[code];
}

function getDbForLead(lead) {
  const country = detectCountry(lead);
  if (country === 'USA') return { db: null, country: 'USA' }; // null means use main db
  const db = getCountryDb(country);
  return { db, country };
}

function listCountryDbs() {
  const files = fs.readdirSync(COUNTRIES_DIR).filter(f => f.endsWith('.db'));
  return files.map(f => ({
    file: f,
    path: path.join(COUNTRIES_DIR, f),
    size: fs.statSync(path.join(COUNTRIES_DIR, f)).size
  }));
}

module.exports = { detectCountry, getCountryDb, getDbForLead, listCountryDbs };
