import { useEffect, useMemo, useState } from 'react';
import { SiteShell } from './SiteHeader';
import { useUrlParams, SearchBox, SortSelect, DirectoryHeading } from './Directory';
import { API_BASE_URL } from '../config';
import { showHref, avatarUrl, coverUrl, fmtMonthYear, plural, setMeta, slugify, publicFetch } from '../profileUtils';

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
    publicFetch(`${API_BASE_URL}/api/directory/shows`)
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
          <ul className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2 sm:gap-3">
            {shown.map(s => (
              <li key={s.podcast_id} className="min-w-0">
                <a href={showHref(s.podcast_id, s.slug)} title={s.title}
                   className="h-full flex items-center gap-3 bg-white rounded-xl border border-gray-200 p-2 pr-3 hover:border-teal-400 hover:shadow-sm transition">
                  <img src={s.cover_art_url ? coverUrl(s.cover_art_url, 160) : avatarUrl(s.title)} alt="" loading="lazy"
                    onError={e => { e.target.onerror = null; e.target.src = avatarUrl(s.title); }}
                    className="w-16 h-16 rounded-lg object-cover bg-gray-100 flex-shrink-0" />
                  <div className="min-w-0">
                    <p className="text-sm font-semibold text-gray-900 truncate">{s.title}</p>
                    {s.channel && s.channel !== s.title && <p className="text-xs text-gray-500 truncate">{s.channel}</p>}
                    <p className="text-xs text-gray-600 truncate">
                      {plural(s.episodes, 'episode')} · {plural(s.guests, 'guest')}
                      {s.last_date && <span className="text-gray-400"> · {fmtMonthYear(s.last_date)}</span>}
                    </p>
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
