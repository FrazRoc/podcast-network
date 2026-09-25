import { useState, useEffect } from 'react';
import { API_BASE_URL } from '../config';
import { ORG_TYPE_COLORS, ORG_TYPE_LABELS } from '../chartUtils';
import OrgLogo from './OrgLogo';

// The organisations whose people are booked most, counted by distinct
// guests or by distinct shows, with sub-organisations under their parent.
// By default a guest on their own organisation's show (BNEF analysts on
// Switched On) isn't counted — that's the house, not a booking.
const PAGE = 15;
// Filter chips, in the order the type colours are listed.
const TYPE_ORDER = ['company', 'investor', 'nonprofit', 'research', 'academic', 'government', 'media', 'association'];

export default function TopOrganizationsChart() {
  const [by, setBy] = useState('guests');
  const [excludeHouse, setExcludeHouse] = useState(true);
  const [orgType, setOrgType] = useState('');
  const [data, setData] = useState(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState(null);

  const url = (offset) => `${API_BASE_URL}/api/stats/top-organizations?by=${by}&exclude_in_house=${excludeHouse}` +
    `&org_type=${orgType}&limit=${PAGE}&offset=${offset}`;

  useEffect(() => {
    setData(null);
    fetch(url(0))
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [by, excludeHouse, orgType]);

  const loadMore = () => {
    setLoadingMore(true);
    fetch(url(data.items.length))
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(d => setData(prev => ({ ...d, items: [...prev.items, ...d.items] })))
      .catch(e => setError(e.message))
      .finally(() => setLoadingMore(false));
  };

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;

  const value = o => (by === 'shows' ? o.shows : o.guests);
  // The first row is the largest of everything loaded, so bars keep their
  // scale as more rows are added.
  const peak = data && data.items.length ? Math.max(1, value(data.items[0])) : 1;
  const types = data ? [...new Set(data.items.map(o => o.org_type).filter(Boolean))] : [];
  const counts = data?.type_counts || {};
  const allCount = Object.values(counts).reduce((a, b) => a + b, 0);

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
      <div className="flex flex-wrap gap-1.5 mb-4 text-xs">
        {[['', 'All', allCount], ...TYPE_ORDER.map(t => [t, ORG_TYPE_LABELS[t], counts[t] || 0])].map(([t, label, n]) => (
          <button key={t || 'all'} onClick={() => setOrgType(t)} disabled={t && !n && orgType !== t}
            className={`flex items-center gap-1.5 px-2 py-1 rounded-full border disabled:opacity-40 ${
              orgType === t ? 'border-gray-900 bg-gray-900 text-white' : 'border-gray-200 text-gray-600 hover:bg-gray-50'}`}>
            {t && <span className="w-2 h-2 rounded-sm" style={{ background: ORG_TYPE_COLORS[t] }} />}
            {label}
            {data && <span className={orgType === t ? 'text-gray-300' : 'text-gray-400'}>{n}</span>}
          </button>
        ))}
      </div>

      {!data ? <p className="text-sm text-gray-400">Loading…</p> : (
        <>
          <div className="space-y-1">
            {data.items.map((o, i) => (
              <div key={o.org_id} className="flex items-center gap-2">
                <span className="w-5 text-right text-[11px] text-gray-400 tabular-nums">{i + 1}</span>
                <span className="w-40 sm:w-56 flex-shrink-0 text-xs text-gray-800 truncate flex items-center gap-1.5" title={o.name}>
                  <OrgLogo orgId={o.org_id} name={o.name} size={16} />
                  <span className="truncate">{o.name}</span>
                </span>
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
          {data.items.length === 0 && <p className="text-sm text-gray-400">No organisations of this type yet.</p>}
          {data.items.length < data.total && (
            <button onClick={loadMore} disabled={loadingMore}
              className="mt-3 text-xs px-3 py-1.5 rounded-lg border border-gray-200 text-gray-600 hover:bg-gray-50 disabled:opacity-50">
              {loadingMore ? 'Loading…' : `Load ${Math.min(PAGE, data.total - data.items.length)} more`}
              <span className="text-gray-400"> · {data.items.length} of {data.total}</span>
            </button>
          )}
          <div className="flex flex-wrap gap-3 mt-3">
            {!orgType && types.map(t => (
              <span key={t} className="flex items-center gap-1.5 text-xs text-gray-500">
                <span className="w-2.5 h-2.5 rounded-sm" style={{ background: ORG_TYPE_COLORS[t] }} />{ORG_TYPE_LABELS[t]}
              </span>
            ))}
          </div>
          <p className="text-xs text-gray-400 mt-3">
            Guests with a current role there; departments, labs and offices count under their parent
            (DOE's offices under DOE, BNEF under Bloomberg). A show's own organisation is inferred from who it
            books most.
          </p>
        </>
      )}
    </div>
  );
}
