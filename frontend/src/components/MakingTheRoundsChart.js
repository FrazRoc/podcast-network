import { useState, useEffect } from 'react';
import { API_BASE_URL } from '../config';
import { personHref, showHref, slugify, imageUrl, imageFallback, avatarUrl, fmtDate, plural } from '../profileUtils';

const DAY = 86400000;
const PAGE = 8;

// "2025-06-25" as a local date, so the strip's day offsets aren't skewed by
// the time zone.
const toDate = (s) => { const [y, m, d] = s.split('-').map(Number); return new Date(y, m - 1, d); };

export default function MakingTheRoundsChart() {
  const [year, setYear] = useState(0);
  const [minShows, setMinShows] = useState(3);
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [shown, setShown] = useState(PAGE);

  useEffect(() => {
    setData(null);
    setShown(PAGE);
    fetch(`${API_BASE_URL}/api/stats/making-the-rounds?min_shows=${minShows}${year ? `&year=${year}` : ''}`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, [year, minShows]);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;

  // The year list comes with the data; keep the last one while reloading so
  // the pills don't vanish and reappear on every click.
  const years = data?.years;
  return (
    <div>
      <div className="flex flex-wrap items-center gap-3 mb-4 text-xs">
        <div className="flex rounded-lg bg-gray-100 p-0.5">
          {[[3, '3+ shows'], [4, '4+ shows']].map(([n, label]) => (
            <button key={n} onClick={() => setMinShows(n)}
              className={`px-2.5 py-1 rounded-md ${minShows === n ? 'bg-white shadow-sm text-gray-900' : 'text-gray-500'}`}>
              {label}
            </button>
          ))}
        </div>
        {years && (
          <div className="flex flex-wrap gap-1.5">
            {[0, ...years].map(y => (
              <button key={y} onClick={() => setYear(y)}
                className={`px-2 py-1 rounded-full border ${year === y
                  ? 'bg-gray-900 border-gray-900 text-white' : 'border-gray-200 text-gray-600 hover:bg-gray-50'}`}>
                {y || 'All years'}
              </button>
            ))}
          </div>
        )}
      </div>

      {!data ? <p className="text-sm text-gray-400">Loading…</p> : data.bursts.length === 0 ? (
        <p className="text-sm text-gray-400">Nobody did the rounds on {minShows}+ shows {year ? `in ${year}` : 'yet'}.</p>
      ) : (
        <>
          <div className="space-y-3">
            {data.bursts.slice(0, shown).map(b => (
              <Burst key={`${b.host_id}-${b.start}`} b={b} windowDays={data.window_days} />
            ))}
          </div>
          {data.bursts.length > shown && (
            <button onClick={() => setShown(s => s + PAGE * 2)}
              className="mt-3 text-xs px-3 py-1.5 rounded-lg border border-gray-200 text-gray-600 hover:bg-gray-50">
              Show more <span className="text-gray-400">· {shown} of {data.bursts.length}</span>
            </button>
          )}
        </>
      )}
      <p className="text-xs text-gray-400 mt-3">
        Guest appearances only, not counting a person's own shows or episodes that only mention them.
        Recent weeks fill in as new episodes are read.
      </p>
    </div>
  );
}

function Burst({ b, windowDays }) {
  const start = toDate(b.start);
  const days = Math.round((toDate(b.end) - start) / DAY);
  const img = b.profile_image_url ? imageUrl(b.profile_image_url, 80) : avatarUrl(b.name);
  return (
    <div className="rounded-xl border border-gray-100 p-3">
      <div className="flex items-center gap-3">
        <img src={img} onError={imageFallback(b.profile_image_url, b.name)} alt=""
          className="w-9 h-9 rounded-full object-cover bg-gray-100 flex-shrink-0" />
        <div className="min-w-0 flex-1">
          <a href={personHref(b.host_id, slugify(b.name))}
            className="text-sm font-medium text-gray-900 hover:text-teal-700 hover:underline">{b.name}</a>
          <p className="text-xs text-gray-500">
            <b className="text-gray-800">{plural(b.shows, 'show')}</b> in {days === 0 ? 'one day' : plural(days + 1, 'day')}
            {' · '}{fmtDate(b.start, { month: 'short', day: 'numeric' })} – {fmtDate(b.end)}
          </p>
        </div>
      </div>

      {/* The window as a strip: one dot per appearance, so a cluster in one
          week reads differently from three spread across the month. */}
      <div className="relative h-3 my-2.5 mx-1">
        <div className="absolute inset-x-0 top-1/2 h-px bg-gray-200" />
        {b.appearances.map(a => {
          const x = ((toDate(a.date) - start) / DAY) / windowDays * 100;
          return (
            <span key={a.episode_id} title={`${fmtDate(a.date)} · ${a.show}`}
              className="absolute top-1/2 w-2.5 h-2.5 -mt-[5px] -ml-[5px] rounded-full bg-teal-600 ring-2 ring-white"
              style={{ left: `${x}%` }} />
          );
        })}
      </div>

      <ul className="space-y-0.5">
        {b.appearances.map(a => (
          <li key={a.episode_id} className="flex gap-2 text-xs min-w-0">
            <span className="w-12 flex-shrink-0 text-gray-400 tabular-nums">{fmtDate(a.date, { month: 'short', day: 'numeric' })}</span>
            <span className="min-w-0 truncate">
              <a href={showHref(a.podcast_id, slugify(a.show))} className="text-gray-800 hover:text-teal-700 hover:underline">{a.show}</a>
              <span className="text-gray-500"> — {a.title}</span>
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}
