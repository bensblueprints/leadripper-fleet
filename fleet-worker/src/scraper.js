/**
 * Embedded Google Maps Scraper for Fleet Worker
 * Uses playwright-core for minimal install footprint.
 */

const { chromium } = require('playwright-core');
const os = require('os');
const path = require('path');
const fs = require('fs');

let browserInstance = null;

function findBrowserExecutable() {
  const isWin = process.platform === 'win32';
  const isMac = process.platform === 'darwin';

  const candidates = [];
  if (isMac) {
    candidates.push(
      '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
      '/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary',
      '/Applications/Chromium.app/Contents/MacOS/Chromium',
      '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
      '/usr/bin/chromium',
      '/usr/bin/chromium-browser'
    );
  } else if (isWin) {
    const pf = process.env['ProgramFiles(x86)'] || process.env.ProgramFiles || 'C:\\Program Files';
    candidates.push(
      path.join(pf, 'Google', 'Chrome', 'Application', 'chrome.exe'),
      path.join(pf, 'Microsoft', 'Edge', 'Application', 'msedge.exe'),
      'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
      'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe'
    );
  } else {
    candidates.push(
      '/usr/bin/chromium',
      '/usr/bin/chromium-browser',
      '/usr/bin/google-chrome',
      '/usr/bin/chrome'
    );
  }

  candidates.push(process.env.PW_CHROMIUM_PATH, process.env.CHROME_PATH);

  for (const p of candidates) {
    if (p && fs.existsSync(p)) return p;
  }
  return null;
}

async function getBrowser() {
  if (browserInstance && browserInstance.isConnected()) return browserInstance;

  const executablePath = findBrowserExecutable();

  if (!executablePath) {
    throw new Error(
      'No Chrome/Chromium/Edge browser found. Please install Google Chrome or set PW_CHROMIUM_PATH environment variable.'
    );
  }

  const launchOptions = {
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--disable-gpu',
      '--disable-background-timer-throttling',
      '--disable-renderer-backgrounding',
      '--disable-backgrounding-occluded-windows',
    ],
    executablePath,
  };

  browserInstance = await chromium.launch(launchOptions);
  return browserInstance;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function extractState(address) {
  if (!address) return '';
  const stateMatch = address.match(/,\s*([A-Z]{2})\s*\d{5}/);
  if (stateMatch) return stateMatch[1];
  const states = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'];
  for (const st of states) {
    if (address.includes(', ' + st + ' ') || address.endsWith(', ' + st)) return st;
  }
  return '';
}

async function scrapeGoogleMaps(query, maxResults = 20) {
  let context;
  try {
    const browser = await getBrowser();

    context = await browser.newContext({
      viewport: { width: 1920, height: 1080 },
      userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      locale: 'en-US',
      timezoneId: 'America/New_York',
    });

    const page = await context.newPage();

    const searchUrl = `https://www.google.com/maps/search/${encodeURIComponent(query)}`;
    await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

    // Accept cookies if prompted
    try {
      const acceptBtn = page.locator('button[aria-label="Accept all"]');
      if (await acceptBtn.isVisible({ timeout: 3000 })) {
        await acceptBtn.click();
      }
    } catch {}

    // Wait for results feed
    try {
      await page.waitForSelector('[role="feed"]', { timeout: 10000 });
    } catch {
      return [];
    }
    await sleep(2000);

    // Scroll to load results
    let prevCount = 0, staleCount = 0;
    for (let s = 0; s < 100; s++) {
      const count = await page.evaluate(() =>
        document.querySelectorAll('[role="feed"] > div > div > a[href*="/maps/place/"]').length
      );
      if (count >= maxResults) break;

      const endReached = await page.evaluate(() => {
        const feed = document.querySelector('[role="feed"]');
        const last = feed?.lastElementChild;
        return last?.textContent?.includes("You've reached the end") || last?.textContent?.includes("end of the list");
      });
      if (endReached) break;

      if (count === prevCount) {
        staleCount++;
        if (staleCount > 5) break;
      } else {
        staleCount = 0;
      }
      prevCount = count;

      await page.evaluate(() => document.querySelector('[role="feed"]')?.scrollBy(0, 2000));
      await sleep(800 + Math.random() * 400);
    }

    // Extract basic info from list view
    const listLeads = await page.evaluate((maxResults) => {
      const results = [];
      const cards = document.querySelectorAll('[role="feed"] > div > div');

      for (const card of cards) {
        const link = card.querySelector('a[href*="/maps/place/"]');
        if (!link || results.length >= maxResults) continue;

        const name = link.getAttribute('aria-label') || '';
        const href = link.href || '';

        let rating = 0, reviews = 0;
        card.querySelectorAll('span').forEach(span => {
          const al = span.getAttribute('aria-label') || '';
          if (al.match(/star/i)) {
            const rm = al.match(/([\d.]+)\s*star/i);
            if (rm) rating = parseFloat(rm[1]);
            const revm = al.match(/([\d,]+)\s*[Rr]eview/);
            if (revm) reviews = parseInt(revm[1].replace(/,/g, ''));
          }
          if (!reviews && span.getAttribute('aria-hidden') === 'true') {
            const txt = span.textContent.trim();
            if (txt.match(/^\([\d,]+\)$/)) reviews = parseInt(txt.replace(/[(),]/g, ''));
          }
        });

        const ct = card.textContent || '';
        let address = '';
        const am = ct.match(/(\d+\s+(?:[NSEW]\s+)?[\w\s]+(?:St|Ave|Blvd|Rd|Dr|Ln|Way|Ct|Pl|Hwy|Pkwy|Road|Drive|Lane|Circle)(?:\s+\w+)?(?:\s*#\s*\w+)?(?:,\s*[\w\s]+)?(?:,\s*[A-Z]{2}\s*\d{5})?)/i);
        if (am) address = am[1].trim();

        let phone = '';
        const pm = ct.match(/\+?1?\s*\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})/);
        if (pm) phone = `(${pm[1]}) ${pm[2]}-${pm[3]}`;

        results.push({
          business_name: name,
          phone,
          address,
          website: '',
          rating,
          reviews,
          google_maps_url: href,
          source_url: href,
        });
      }

      return results;
    }, maxResults);

    // Deep scrape: click into each listing
    for (let i = 0; i < listLeads.length; i++) {
      try {
        await page.goto(listLeads[i].google_maps_url, { waitUntil: 'domcontentloaded', timeout: 8000 });
        await sleep(1000 + Math.random() * 500);

        const enriched = await page.evaluate(() => {
          let phone = '', website = '', hours = '', fullAddress = '';

          document.querySelectorAll('button[data-item-id], a[data-item-id]').forEach(el => {
            const id = el.getAttribute('data-item-id') || '';
            if (id.startsWith('phone')) phone = el.textContent.trim();
            if (id === 'authority') website = el.href || '';
            if (id.startsWith('oh')) hours = el.getAttribute('aria-label') || '';
            if (id === 'address' || id.startsWith('address')) fullAddress = el.textContent.trim();
          });

          if (!website) {
            const w = document.querySelector('a[aria-label*="ebsite" i]');
            if (w) website = w.href;
          }

          if (!phone) {
            const phoneBtn = document.querySelector('button[aria-label*="Phone"]');
            if (phoneBtn) {
              const label = phoneBtn.getAttribute('aria-label') || '';
              const match = label.match(/Phone:\s*([\d\-\(\)\s\+]+)/i);
              if (match) phone = match[1].trim();
            }
          }

          let reviews = 0;
          const re = document.querySelector('div.F7nice span[aria-label*="review"]');
          if (re) {
            const m = re.textContent.match(/[\d,]+/);
            if (m) reviews = parseInt(m[0].replace(/,/g, ''));
          }

          let ownerName = '';
          const ownerEl = document.querySelector('[data-attrid*="owner"], [aria-label*="Owner"]');
          if (ownerEl) ownerName = ownerEl.textContent.replace(/Owner:?\s*/i, '').trim();

          return { phone, website, hours, fullAddress, reviews, ownerName };
        });

        if (enriched.phone) {
          listLeads[i].phone = enriched.phone.replace(/[^\x20-\x7E+]/g, '').trim();
        }
        if (enriched.website) {
          listLeads[i].website = enriched.website;
        }
        if (enriched.fullAddress) {
          listLeads[i].address = enriched.fullAddress.replace(/[^\x20-\x7E,#.]/g, '').trim();
        }
        if (enriched.hours) {
          listLeads[i].business_hours = enriched.hours;
        }
        if (enriched.reviews) {
          listLeads[i].reviews = enriched.reviews;
        }
        if (enriched.ownerName) {
          listLeads[i].contact_name = enriched.ownerName;
        }
      } catch (e) {
        // Skip failed enrichments
      }

      // Parse city/state from address
      if (listLeads[i].address) {
        const parts = listLeads[i].address.split(',').map(p => p.trim());
        if (parts.length >= 3) {
          listLeads[i].city = parts[parts.length - 3] || '';
          const sz = (parts[parts.length - 2] || '').match(/^([A-Z]{2})\s*(\d{5})?/);
          if (sz) {
            listLeads[i].state = sz[1];
            listLeads[i].zip = sz[2] || '';
          }
        }
      }
      listLeads[i].city = listLeads[i].city || '';
      listLeads[i].state = listLeads[i].state || '';
      listLeads[i].email = '';
      listLeads[i].industry = '';
      listLeads[i].source = 'google_maps';
    }

    return listLeads;
  } catch (error) {
    throw error;
  } finally {
    if (context) await context.close().catch(() => {});
  }
}

async function closeBrowser() {
  if (browserInstance) {
    await browserInstance.close().catch(() => {});
    browserInstance = null;
  }
}

module.exports = { scrapeGoogleMaps, closeBrowser };
