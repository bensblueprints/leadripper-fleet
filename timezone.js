// State → IANA timezone (per Lead Ripper spec section 5).
// First entry is the canonical timezone for each state. Multi-zone states
// (AK, FL, KY, IN, KS, MI, NE, ND, OR, SD, TN, TX) collapse to the dominant zone.
const STATE_TZ = {
  AL: 'America/Chicago', AK: 'America/Anchorage', AZ: 'America/Phoenix',
  AR: 'America/Chicago', CA: 'America/Los_Angeles', CO: 'America/Denver',
  CT: 'America/New_York', DE: 'America/New_York', FL: 'America/New_York',
  GA: 'America/New_York', HI: 'Pacific/Honolulu', ID: 'America/Boise',
  IL: 'America/Chicago', IN: 'America/Indiana/Indianapolis', IA: 'America/Chicago',
  KS: 'America/Chicago', KY: 'America/New_York', LA: 'America/Chicago',
  ME: 'America/New_York', MD: 'America/New_York', MA: 'America/New_York',
  MI: 'America/Detroit', MN: 'America/Chicago', MS: 'America/Chicago',
  MO: 'America/Chicago', MT: 'America/Denver', NE: 'America/Chicago',
  NV: 'America/Los_Angeles', NH: 'America/New_York', NJ: 'America/New_York',
  NM: 'America/Denver', NY: 'America/New_York', NC: 'America/New_York',
  ND: 'America/Chicago', OH: 'America/New_York', OK: 'America/Chicago',
  OR: 'America/Los_Angeles', PA: 'America/New_York', RI: 'America/New_York',
  SC: 'America/New_York', SD: 'America/Chicago', TN: 'America/Chicago',
  TX: 'America/Chicago', UT: 'America/Denver', VT: 'America/New_York',
  VA: 'America/New_York', WA: 'America/Los_Angeles', WV: 'America/New_York',
  WI: 'America/Chicago', WY: 'America/Denver',
  DC: 'America/New_York', PR: 'America/Puerto_Rico', VI: 'America/St_Thomas',
  GU: 'Pacific/Guam', AS: 'Pacific/Pago_Pago', MP: 'Pacific/Saipan'
};

function timezoneForState(state) {
  if (!state) return null;
  return STATE_TZ[String(state).toUpperCase().trim()] || null;
}

const VALID_US_STATES = new Set(Object.keys(STATE_TZ));
function isUsState(state) {
  if (!state) return false;
  return VALID_US_STATES.has(String(state).toUpperCase().trim());
}

module.exports = { timezoneForState, isUsState, STATE_TZ };
