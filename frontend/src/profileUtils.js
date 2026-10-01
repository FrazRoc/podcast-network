import { useState, useEffect } from 'react';
import { API_BASE_URL } from './config';

// Shared by the public person, organisation and show pages
// (/people/<id>-<slug>, /orgs/<id>-<slug>, /shows/<id>-<slug>). The id is
// authoritative; the slug is only there to make URLs readable, so a page
// loaded with an old one rewrites it to the current one.

export const personHref = (id, slug) => `/people/${id}${slug ? `-${slug}` : ''}`;
export const orgHref = (id, slug) => `/orgs/${id}${slug ? `-${slug}` : ''}`;
export const showHref = (id, slug) => `/shows/${id}${slug ? `-${slug}` : ''}`;

// Same client-side slug rule as the backend's profiles.slugify, for links
// built from data that doesn't carry a slug (the graph, the Stats charts).
export const slugify = (name) => {
  const s = (name || '').normalize('NFKD').replace(/[̀-ͯ]/g, '')
    .replace(/ð/gi, 'd').replace(/ø/gi, 'o').replace(/æ/gi, 'ae').replace(/ß/g, 'ss').replace(/ł/gi, 'l')
    .replace(/[^A-Za-z0-9]+/g, '-').replace(/^-+|-+$/g, '').toLowerCase().slice(0, 80).replace(/-+$/, '');
  return s || 'page';
};

export const avatarUrl = (name) =>
  `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(name || '?')}&backgroundColor=65c9ff,92a1c6,dd6b7f,58c9b9,ade498`;

// Remote images go through the backend proxy, as on the Network page.
// A show's cover for an <img>. Apple's artwork (every show's, today) loads
// straight from Apple at the size asked for (the proxy is only needed where
// the graph draws images on a canvas); anything else goes through the proxy.
const APPLE_ART_RE = /^(https:\/\/is\d+-ssl\.mzstatic\.com\/.+\/)\d+x\d+(bb\.(?:jpg|png|webp))$/;
export const coverUrl = (url, size = 160) => {
  if (!url) return null;
  const m = url.match(APPLE_ART_RE);
  return m ? `${m[1]}${size}x${size}${m[2]}` : proxied(url);
};

export const proxied = (url) => (url ? `${API_BASE_URL}/api/proxy/image?url=${encodeURIComponent(url)}` : null);

// A DATE ("2026-09-10") parsed as local, not UTC, so it isn't a day off
// west of UTC (same reason as adminUtils.formatDateOnly).
export const fmtDate = (d, opts = { year: 'numeric', month: 'short', day: 'numeric' }) => {
  if (!d) return '';
  const [y, m, day] = String(d).slice(0, 10).split('-').map(Number);
  return new Date(y, (m || 1) - 1, day || 1).toLocaleDateString(undefined, opts);
};
export const fmtMonthYear = (d) => fmtDate(d, { year: 'numeric', month: 'short' });
export const plural = (n, word, many) => `${Number(n).toLocaleString()} ${n === 1 ? word : (many || `${word}s`)}`;

export const idFromPath = (prefix) => {
  const rest = window.location.pathname.slice(prefix.length).split('/')[0];
  const id = parseInt(rest, 10);
  return Number.isFinite(id) ? id : null;
};

export function setMeta(title, description) {
  document.title = title;
  let tag = document.querySelector('meta[name="description"]');
  if (!tag) {
    tag = document.createElement('meta');
    tag.setAttribute('name', 'description');
    document.head.appendChild(tag);
  }
  tag.setAttribute('content', description || '');
}

// Loads /api/<kind>/<id>/profile, fixes up the slug in the address bar, and
// sets the page title and description. Returns { data, error }.
export function useProfile(kind, prefix, describe) {
  const [state, setState] = useState({ data: null, error: null });
  useEffect(() => {
    const id = idFromPath(prefix);
    if (!id) { setState({ data: null, error: 'not_found' }); return; }
    let cancelled = false;
    fetch(`${API_BASE_URL}/api/${kind}/${id}/profile`)
      .then(r => (r.status === 404 ? Promise.reject(new Error('not_found')) : r.ok ? r.json() : Promise.reject(new Error('error'))))
      .then(data => {
        if (cancelled) return;
        const want = `${prefix}${id}-${data.slug}`;
        if (window.location.pathname !== want) {
          try { window.history.replaceState(null, '', want + window.location.search); } catch { /* sandboxed */ }
        }
        const [title, description] = describe(data);
        setMeta(`${title} · Podcast Network`, description);
        setState({ data, error: null });
      })
      .catch(e => { if (!cancelled) setState({ data: null, error: e.message === 'not_found' ? 'not_found' : 'error' }); });
    return () => { cancelled = true; };
  // describe is a fresh closure each render; the page only loads once.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [kind, prefix]);
  return state;
}
