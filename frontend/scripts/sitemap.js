// Writes public/sitemap.xml before each build (npm runs "prebuild" first),
// from the backend's /api/sitemap. The site is a single-page app, so this is
// how search engines find every person, organisation, show and topic page.
// A backend that can't be reached leaves the main pages only: a stale or
// short sitemap is better than a failed deploy.
const fs = require('fs');
const path = require('path');

const SITE = (process.env.SITE_URL || 'https://podcast-network-frontend.onrender.com').replace(/\/+$/, '');
const API = (process.env.REACT_APP_API_URL || 'https://podcast-network-backend.onrender.com').replace(/\/+$/, '');
const OUT = path.join(__dirname, '..', 'public', 'sitemap.xml');
const MAIN = ['/', '/people', '/orgs', '/shows', '/topics', '/stats'].map(p => ({ path: p, lastmod: null }));

const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

async function pages() {
  if (typeof fetch !== 'function') return MAIN;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 120000);
  try {
    const res = await fetch(`${API}/api/sitemap`, { signal: ctrl.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = await res.json();
    return body.pages && body.pages.length ? body.pages : MAIN;
  } catch (e) {
    console.warn(`sitemap: ${e.message} — writing the main pages only`);
    return MAIN;
  } finally {
    clearTimeout(timer);
  }
}

pages().then(list => {
  const urls = list.slice(0, 50000).map(p =>
    `  <url><loc>${esc(SITE + p.path)}</loc>${p.lastmod ? `<lastmod>${p.lastmod}</lastmod>` : ''}</url>`);
  fs.writeFileSync(OUT, `<?xml version="1.0" encoding="UTF-8"?>\n`
    + `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${urls.join('\n')}\n</urlset>\n`);
  console.log(`sitemap: ${urls.length} pages`);
});
