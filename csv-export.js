// CSV export engine — Lead Ripper spec section 7.
// Server-primary write to /data/LeadRipper/{industry_slug}.csv + master DB CSV + stats.json.
// Append-only with file streams; lock-free per-file (single-writer node process).
//
// Spec columns (order matters):
//   Business Name, Phone Number, Email Address, Contact Name, Contact Title,
//   Website, Website Platform, Address, City, State, ZIP Code, Timezone,
//   Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday, Hours Raw,
//   Industry, First Seen, Source Job, Source Node

const fs = require('fs');
const path = require('path');

const ROOT = process.env.LEADRIPPER_CSV_ROOT || '/data/LeadRipper';
const HEADER = [
  'Business Name', 'Phone Number', 'Email Address', 'Contact Name', 'Contact Title',
  'Website', 'Website Platform', 'Address', 'City', 'State', 'ZIP Code', 'Timezone',
  'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'Hours Raw',
  'Industry', 'First Seen', 'Source Job', 'Source Node'
];

function ensureDir() {
  if (!fs.existsSync(ROOT)) fs.mkdirSync(ROOT, { recursive: true });
}

function industrySlug(industry) {
  return String(industry || 'unknown')
    .replace(/[^a-zA-Z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'unknown';
}

function csvCell(v) {
  if (v == null) return '';
  const s = String(v);
  if (/[",\r\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function csvLine(arr) {
  return arr.map(csvCell).join(',') + '\n';
}

function leadToRow(lead) {
  return [
    lead.business_name || lead.name || '',
    lead.phone_number || lead.phone || '',
    lead.email_address || lead.email || 'no_email_found',
    lead.contact_name || 'no_contact_found',
    lead.contact_title || 'no_contact_found',
    lead.website || 'no_website',
    lead.website_platform || (lead.website ? '' : 'no_website'),
    lead.address || '',
    lead.city || '',
    lead.state || '',
    lead.zip_code || '',
    lead.timezone || '',
    lead.hours_monday || 'not_available',
    lead.hours_tuesday || 'not_available',
    lead.hours_wednesday || 'not_available',
    lead.hours_thursday || 'not_available',
    lead.hours_friday || 'not_available',
    lead.hours_saturday || 'not_available',
    lead.hours_sunday || 'not_available',
    lead.hours_raw || lead.business_hours || '',
    lead.industry || '',
    lead.first_seen || new Date().toISOString().slice(0, 10),
    lead.source_job_id || lead.job_id || '',
    lead.source_node || lead.node_label || ''
  ];
}

function appendIfMissing(filePath, headerLine) {
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, headerLine);
    return true;
  }
  return false;
}

const headerLine = csvLine(HEADER);

// Append a batch of approved leads. Writes per-industry file + master file in one pass.
// leads: [{...lead fields}], grouped by industry not required.
function appendApprovedLeads(leads) {
  if (!leads || !leads.length) return { written: 0 };
  ensureDir();

  // Group by industry slug
  const byInd = new Map();
  for (const l of leads) {
    const slug = industrySlug(l.industry);
    if (!byInd.has(slug)) byInd.set(slug, []);
    byInd.get(slug).push(l);
  }

  // Per-industry files
  for (const [slug, group] of byInd) {
    const file = path.join(ROOT, slug + '.csv');
    appendIfMissing(file, headerLine);
    let buf = '';
    for (const l of group) buf += csvLine(leadToRow(l));
    fs.appendFileSync(file, buf);
  }

  // Master DB
  const master = path.join(ROOT, '_master_database.csv');
  appendIfMissing(master, headerLine);
  let mbuf = '';
  for (const l of leads) mbuf += csvLine(leadToRow(l));
  fs.appendFileSync(master, mbuf);

  return { written: leads.length, industries: byInd.size };
}

// Stats: { "industry_slug": count, ... } at /data/LeadRipper/_stats.json
function bumpStats(perIndustryCounts) {
  ensureDir();
  const file = path.join(ROOT, '_stats.json');
  let s = {};
  try { s = JSON.parse(fs.readFileSync(file, 'utf8')); } catch {}
  for (const [slug, n] of Object.entries(perIndustryCounts)) {
    s[slug] = (s[slug] || 0) + n;
  }
  s._updated_at = new Date().toISOString();
  fs.writeFileSync(file, JSON.stringify(s, null, 2));
}

// Append job to _completed_jobs.csv (immediate persistence per spec).
const COMPLETED_HEADER = csvLine(['job_id', 'industry', 'city', 'state', 'completed_at', 'leads_found', 'leads_approved', 'leads_rejected', 'node_id']);
function appendCompletedJob(row) {
  ensureDir();
  const file = path.join(ROOT, '_completed_jobs.csv');
  appendIfMissing(file, COMPLETED_HEADER);
  fs.appendFileSync(file, csvLine([
    row.job_id, row.industry, row.city, row.state,
    new Date((row.completed_at || Date.now() / 1000) * 1000).toISOString(),
    row.leads_found || 0, row.leads_approved || 0, row.leads_rejected || 0,
    row.node_id || ''
  ]));
}

module.exports = {
  appendApprovedLeads,
  appendCompletedJob,
  bumpStats,
  industrySlug,
  ROOT
};
