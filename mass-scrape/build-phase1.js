#!/usr/bin/env node
// One-shot script: joins us-cities.json against a SimpleMaps population CSV
// (free CC-BY license) and writes cities-phase1.json (pop >= 10k) and
// cities-phase2.json (everything else).
//
// Usage:
//   1) Download "uscities.csv" from https://simplemaps.com/data/us-cities (Basic, free)
//   2) Place it next to this script (or pass --csv=/path/to/uscities.csv)
//   3) node build-phase1.js
//
// Writes: cities-phase1.json, cities-phase2.json in this directory.
//
// The pop join is "best effort". If a city is missing from the SimpleMaps CSV,
// it falls into phase2.

const fs = require('fs');
const path = require('path');

const args = Object.fromEntries(process.argv.slice(2).map(a => {
  const m = a.match(/^--([^=]+)=?(.*)$/);
  return m ? [m[1], m[2] || true] : null;
}).filter(Boolean));

const csvPath = args.csv || path.join(__dirname, 'uscities.csv');
const citiesPath = args.cities || path.join(__dirname, '..', 'public', 'us-cities.json');
const threshold = +args.threshold || 10000;
const phase1Out = path.join(__dirname, 'cities-phase1.json');
const phase2Out = path.join(__dirname, 'cities-phase2.json');

if (!fs.existsSync(csvPath)) {
  console.error(`Missing ${csvPath}`);
  console.error('Download SimpleMaps US Cities Basic (free) from https://simplemaps.com/data/us-cities');
  process.exit(1);
}
if (!fs.existsSync(citiesPath)) {
  console.error(`Missing ${citiesPath}`);
  process.exit(1);
}

function parseCsv(text) {
  // Minimal CSV parser for SimpleMaps format (fields may be quoted).
  const lines = text.split(/\r?\n/).filter(Boolean);
  const header = splitLine(lines.shift());
  const out = [];
  for (const line of lines) {
    const cols = splitLine(line);
    const row = {};
    header.forEach((h, i) => row[h] = cols[i]);
    out.push(row);
  }
  return out;
}
function splitLine(line) {
  const out = [];
  let cur = '';
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { q = !q; continue; }
    if (c === ',' && !q) { out.push(cur); cur = ''; continue; }
    cur += c;
  }
  out.push(cur);
  return out;
}

const csv = parseCsv(fs.readFileSync(csvPath, 'utf8'));
// Build "City, ST" → population map (collapse duplicates, keep max population)
const popMap = new Map();
for (const r of csv) {
  const city = (r.city || '').trim();
  const st = (r.state_id || '').trim();
  if (!city || !st) continue;
  const key = `${city}, ${st}`;
  const pop = parseInt(r.population || '0', 10) || 0;
  const prev = popMap.get(key) || 0;
  if (pop > prev) popMap.set(key, pop);
}

const rawCities = JSON.parse(fs.readFileSync(citiesPath, 'utf8'));
const cities = Array.isArray(rawCities)
  ? rawCities.map(c => (typeof c === 'string' ? c : (c.city_state || `${c.city}, ${c.state}`))).filter(Boolean)
  : [];

const phase1 = [];
const phase2 = [];
for (const cs of cities) {
  const pop = popMap.get(cs) || 0;
  const parts = cs.split(',').map(x => x.trim());
  const entry = { city_state: cs, city: parts[0], state: parts[1] || '', population: pop };
  if (pop >= threshold) phase1.push(entry);
  else phase2.push(entry);
}

phase1.sort((a, b) => b.population - a.population);
phase2.sort((a, b) => b.population - a.population);

fs.writeFileSync(phase1Out, JSON.stringify(phase1, null, 2));
fs.writeFileSync(phase2Out, JSON.stringify(phase2, null, 2));
console.log(`phase1: ${phase1.length} cities (pop >= ${threshold})`);
console.log(`phase2: ${phase2.length} cities (pop < ${threshold} or unknown)`);
console.log(`wrote ${phase1Out}`);
console.log(`wrote ${phase2Out}`);
