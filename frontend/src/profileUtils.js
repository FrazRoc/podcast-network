import { useState, useEffect } from 'react';
import { API_BASE_URL } from './config';
import { getAdminPassword } from './adminAuth';

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
// An image for an <img>, at about the size it's shown (`size` in pixels;
// pass ~2x the CSS size for sharp screens). Hosts that serve resized copies
// load straight from the source: Apple's artwork, Bluesky avatars and
// Wikimedia Commons. Anything else (unavatar.io, mostly) goes through our
// proxy, which the graph also needs for drawing on its canvas.
const APPLE_ART_RE = /^(https:\/\/is\d+-ssl\.mzstatic\.com\/.+\/)\d+x\d+([a-z]{2}\.(?:jpg|png|webp))$/;
const BSKY_AVATAR = 'https://cdn.bsky.app/img/avatar/';
const COMMONS_FILE_RE = /^(https:\/\/commons\.wikimedia\.org\/wiki\/Special:FilePath\/[^?]+)(?:\?.*)?$/;
const COMMONS_WIDTHS = [60, 120, 250, 500];   // Wikimedia's standard thumbnail steps
export const imageUrl = (url, size = 160) => {
  if (!url) return null;
  let m = url.match(APPLE_ART_RE);
  if (m) return `${m[1]}${size}x${size}${m[2]}`;
  if (url.startsWith(BSKY_AVATAR)) return size <= 128 ? url.replace(BSKY_AVATAR, 'https://cdn.bsky.app/img/avatar_thumbnail/') : url;
  m = url.match(COMMONS_FILE_RE);
  if (m) return `${m[1]}?width=${COMMONS_WIDTHS.find(w => w >= size) || 500}`;
  return `${proxied(url)}&w=${size}`;   // the proxy shrinks it
};
export const coverUrl = imageUrl;

// onError for an image loaded through imageUrl(): retry once through the
// proxy (a host may refuse a direct load), then fall back to initials.
export const imageFallback = (url, name) => (e) => {
  const img = e.currentTarget;
  if (url && !img.dataset.viaProxy && img.src !== proxied(url)) {
    img.dataset.viaProxy = '1';
    img.src = proxied(url);
  } else {
    img.onerror = null;
    img.src = avatarUrl(name);
  }
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

// Public API reads. The server lets browsers cache these for a few minutes;
// a browser logged into admin always asks again, so an edit made there shows
// up on the next page load.
const isAdminBrowser = () => { try { return !!getAdminPassword(); } catch { return false; } };
export const publicFetch = (url) => fetch(url, isAdminBrowser() ? { cache: 'no-cache' } : undefined);

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
    publicFetch(`${API_BASE_URL}/api/${kind}/${id}/profile`)
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
