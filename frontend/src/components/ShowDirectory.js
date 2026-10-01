import { useEffect, useMemo, useState } from 'react';
import { SiteShell } from './SiteHeader';
import { useUrlParams, SearchBox, SortSelect, DirectoryHeading } from './Directory';
import { API_BASE_URL } from '../config';
import { showHref, avatarUrl, proxied, fmtMonthYear, plural, setMeta, slugify } from '../profileUtils';

const SORTS = [['guests', 'Most guests'], ['recent', 'Most recent'], ['episodes', 'Most episodes'], ['name', 'A–Z']];
const byName = (a, b) => a.title.localeCompare(b.title);
const COMPARE = {
  guests: (a, b) => b.guests - a.guests || byName(a, b),
  episodes: (a, b) => b.episodes - a.episodes || byName(a, b),
  recent: (a, b) => String(b.last_date || '').localeCompare(String(a.last_date || '')) || byName(a, b),
  name: byName,
};

export default function ShowDirectory() {
  useEffect(() => setMeta('Shows · Podcast Network',
    'Every clean-energy and climate podcast we track: who hosts it, how often it publishes, and how many guests it has had.'), []);
  const [params, set] = useUrlParams({ q: '', sort: 'guests' });
  const [state, setState] = useState({ rows: null, error: false });
  useEffect(() => {
    fetch(`${API_BASE_URL}/api/directory/shows`)
      .then(r => (r.ok ? r.json() : Promise.reject(r)))
      .then(d => setState({ rows: d.rows, error: false }))
      .catch(() => setState({ rows: null, error: true }));
  }, []);

  const shown = useMemo(() => {
    if (!state.rows) return [];
    const needle = slugify(params.q).replace(/-/g, ' ');
    const hay = s => slugify(`${s.title} ${s.channel || ''}`).replace(/-/g, ' ');
    return state.rows.filter(s => !params.q || hay(s).includes(needle)).sort(COMPARE[params.sort] || COMPARE.guests);
  }, [state.rows, params.q, params.sort]);

  return (
    <SiteShell width="max-w-6xl">
      <DirectoryHeading title="Shows" blurb="Every podcast we track, with its episodes and the guests it has booked." />
      <div className="flex gap-2 max-w-xl">
        <SearchBox value={params.q} onChange={q => set({ q })} placeholder="Filter by show or network…" />
        <SortSelect value={params.sort} options={SORTS} onChange={sort => set({ sort })} />
      </div>
      {state.error && <p className="text-center text-sm text-gray-500 py-12">Couldn't load the shows. Please try again in a moment.</p>}
      {!state.rows && !state.error && <p className="text-center text-sm text-gray-500 py-12">Loading…</p>}
      {state.rows && (
        <>
          <p className="text-xs text-gray-400">{plural(shown.length, 'show')}</p>
          <ul className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3 sm:gap-4">
            {shown.map(s => (
              <li key={s.podcast_id}>
                <a href={showHref(s.podcast_id, s.slug)}
                   className="block h-full bg-white rounded-xl border border-gray-200 overflow-hidden hover:border-teal-400 hover:shadow-sm transition">
                  <img src={s.cover_art_url ? proxied(s.cover_art_url) : avatarUrl(s.title)} alt="" loading="lazy"
                    onError={e => { e.target.onerror = null; e.target.src = avatarUrl(s.title); }}
                    className="w-full aspect-square object-cover bg-gray-100" />
                  <div className="p-3">
                    <p className="text-sm font-semibold text-gray-900 line-clamp-2">{s.title}</p>
                    {s.channel && s.channel !== s.title && <p className="text-xs text-gray-500 truncate">{s.channel}</p>}
                    <p className="mt-1 text-xs text-gray-600">{plural(s.episodes, 'episode')} · {plural(s.guests, 'guest')}</p>
                    {s.last_date && <p className="text-xs text-gray-400">Latest {fmtMonthYear(s.last_date)}</p>}
                  </div>
                </a>
              </li>
            ))}
          </ul>
          {shown.length === 0 && <p className="text-center text-sm text-gray-500 py-8">No matches.</p>}
        </>
      )}
    </SiteShell>
  );
}
