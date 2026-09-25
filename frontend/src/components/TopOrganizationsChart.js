import { useState, useEffect } from 'react';
import { API_BASE_URL } from '../config';
import { ORG_TYPE_COLORS, ORG_TYPE_LABELS } from '../chartUtils';

// The organisations whose people are booked most, counted by distinct
// guests or by distinct shows, with sub-organisations under their parent.
// By default a guest on their own organisation's show (BNEF analysts on
// Switched On) isn't counted — that's the house, not a booking.
export default function TopOrganizationsChart() {
  const [by, setBy] = useState('guests');
  const [excludeHouse, setExcludeHouse] = useState(true);
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    setData(null);
    fetch(`${API_BASE_URL}/api/stats/top-organizations?by=${by}&exclude_in_house=${excludeHouse}`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, [by, excludeHouse]);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;

  const value = o => (by === 'shows' ? o.shows : o.guests);
  const peak = data ? Math.max(1, ...data.items.map(value)) : 1;
  const types = data ? [...new Set(data.items.map(o => o.org_type).filter(Boolean))] : [];

  return (
    <div>
      <div className="flex flex-wrap items-center gap-3 mb-4 text-xs">
        <div className="flex rounded-lg bg-gray-100 p-0.5">
          {[['guests', 'By guests'], ['shows', 'By shows reached']].map(([id, label]) => (
            <button key={id} onClick={() => setBy(id)}
              className={`px-2.5 py-1 rounded-md ${by === id ? 'bg-white shadow-sm text-gray-900' : 'text-gray-500'}`}>
              {label}
            </button>
          ))}
        </div>
        <label className="flex items-center gap-1.5 text-gray-600">
          <input type="checkbox" checked={excludeHouse} onChange={e => setExcludeHouse(e.target.checked)} />
          Leave out guests on their own organisation's show
        </label>
      </div>

      {!data ? <p className="text-sm text-gray-400">Loading…</p> : (
        <>
          <div className="space-y-1">
            {data.items.map((o, i) => (
              <div key={o.org_id} className="flex items-center gap-2">
                <span className="w-5 text-right text-[11px] text-gray-400 tabular-nums">{i + 1}</span>
                <span className="w-40 sm:w-56 flex-shrink-0 text-xs text-gray-800 truncate" title={o.name}>{o.name}</span>
                <div className="flex-1 h-4">
                  <div className="h-full rounded" title={o.org_type ? ORG_TYPE_LABELS[o.org_type] : 'No type'}
                    style={{ width: `${(100 * value(o)) / peak}%`, background: ORG_TYPE_COLORS[o.org_type] || '#d1d5db' }} />
                </div>
                <span className="w-28 flex-shrink-0 text-[11px] text-gray-500 tabular-nums">
                  {by === 'shows'
                    ? <><b className="text-gray-800">{o.shows}</b> shows · {o.guests} guests</>
                    : <><b className="text-gray-800">{o.guests}</b> guests · {o.shows} shows</>}
                </span>
              </div>
            ))}
          </div>
          <div className="flex flex-wrap gap-3 mt-3">
            {types.map(t => (
              <span key={t} className="flex items-center gap-1.5 text-xs text-gray-500">
                <span className="w-2.5 h-2.5 rounded-sm" style={{ background: ORG_TYPE_COLORS[t] }} />{ORG_TYPE_LABELS[t]}
              </span>
            ))}
          </div>
          <p className="text-xs text-gray-400 mt-3">
            Guests with a current role there; departments, labs and offices count under their parent
            (DOE's offices under DOE, BNEF under Bloomberg). A show's own organisation is inferred from who it
            books most: {data.house_shows.map(h => `${h.org} on ${h.title}`).join(', ')}.
          </p>
        </>
      )}
    </div>
  );
}
