import { useState } from 'react';
import Credits from './Credits';
import ProfileSearch from './ProfileSearch';
import { avatarUrl, proxied } from '../profileUtils';

// A show's cover art (episodes have none of their own, so their rows use it
// too), falling back to an initials tile.
export function ShowThumb({ show, size = 'w-10 h-10' }) {
  return (
    <img src={show.cover_art_url ? proxied(show.cover_art_url) : avatarUrl(show.title)} alt=""
      onError={e => { e.target.onerror = null; e.target.src = avatarUrl(show.title); }}
      className={`${size} rounded object-cover bg-gray-100 flex-shrink-0`} />
  );
}

export function Section({ title, children, aside }) {
  return (
    <section className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
      <div className="flex items-baseline justify-between gap-3 mb-3">
        <h2 className="text-base font-semibold text-gray-900">{title}</h2>
        {aside && <span className="text-xs text-gray-400">{aside}</span>}
      </div>
      {children}
    </section>
  );
}

export function Stat({ label, value }) {
  return (
    <div className="bg-teal-50 rounded-lg px-3 py-2 text-center min-w-0">
      <p className="text-[11px] text-gray-500 uppercase tracking-wide">{label}</p>
      <p className="font-semibold text-gray-900 truncate">{value}</p>
    </div>
  );
}

// Horizontal bars for small counts (airtime by year, episodes per year).
export function MiniBars({ rows, labelKey, valueKey, color = '#0d9488' }) {
  const max = Math.max(1, ...rows.map(r => r[valueKey]));
  return (
    <div className="space-y-1">
      {rows.map(r => (
        <div key={r[labelKey]} className="flex items-center gap-2 text-xs">
          <span className="w-10 text-right text-gray-500 flex-shrink-0">{r[labelKey]}</span>
          <div className="flex-1 h-3 bg-gray-100 rounded">
            <div className="h-3 rounded" style={{ width: `${(100 * r[valueKey]) / max}%`, background: color }} />
          </div>
          <span className="w-8 text-gray-700 flex-shrink-0">{r[valueKey]}</span>
        </div>
      ))}
    </div>
  );
}

export function ShowMore({ items, initial = 10, render, more = 'Show all' }) {
  const [all, setAll] = useState(false);
  const shown = all ? items : items.slice(0, initial);
  return (
    <>
      {shown.map(render)}
      {items.length > initial && (
        <button onClick={() => setAll(a => !a)} className="mt-2 text-sm text-teal-700 hover:underline">
          {all ? 'Show fewer' : `${more} (${items.length})`}
        </button>
      )}
    </>
  );
}

export default function ProfileLayout({ state, kindLabel, children }) {
  const { data, error } = state;
  return (
    <div className="h-screen overflow-y-auto bg-gray-100 font-sans text-left">
      <header className="bg-white border-b border-gray-200 px-4 py-3 sm:px-6 flex flex-wrap items-center gap-x-4 gap-y-2">
        <a href="/" className="text-gray-400 hover:text-gray-600 text-sm">← Network</a>
        <a href="/stats" className="text-gray-400 hover:text-gray-600 text-sm">Stats</a>
        <ProfileSearch className="w-full sm:w-80 sm:ml-auto" />
      </header>
      <main className="max-w-4xl mx-auto p-4 sm:p-6 space-y-5">
        {error === 'not_found' && (
          <div className="bg-white rounded-2xl border border-gray-200 p-8 text-center">
            <p className="text-lg font-semibold text-gray-900">{kindLabel} not found</p>
            <p className="text-sm text-gray-500 mt-1">It may have been merged into another record. Try searching above.</p>
          </div>
        )}
        {error === 'error' && (
          <p className="text-center text-sm text-gray-500 py-12">Couldn't load this page. Please try again in a moment.</p>
        )}
        {!data && !error && <p className="text-center text-sm text-gray-500 py-12">Loading…</p>}
        {data && children(data)}
        <Credits className="text-center pt-2" />
      </main>
    </div>
  );
}
