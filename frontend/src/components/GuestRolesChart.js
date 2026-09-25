import { useState, useEffect } from 'react';
import { API_BASE_URL } from '../config';
import { ROLE_COLORS } from '../chartUtils';
import StackedShareBars from './StackedShareBars';

// What guests do: every guest counted once by the kind of role their
// current title describes, across the network and per show.
export default function GuestRolesChart() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/guest-roles`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading…</p>;

  const peak = Math.max(1, ...data.kinds.map(k => data.overall[k] || 0));
  const rows = data.shows.map(s => ({ id: s.podcast_id, label: s.title, counts: s.counts, total: s.total }));
  // Most guests first; the catch-all "other" stays at the bottom.
  const byCount = [...data.kinds].sort((a, b) =>
    (a === 'other') - (b === 'other') || (data.overall[b] || 0) - (data.overall[a] || 0));

  return (
    <div>
      <p className="text-xs font-medium text-gray-500 mb-2">Across the network · {data.total.toLocaleString()} guests</p>
      <div className="space-y-1 mb-6">
        {byCount.map(k => (
          <div key={k} className="flex items-center gap-2">
            <span className="w-40 sm:w-52 flex-shrink-0 text-xs text-gray-700 text-right">{data.labels[k]}</span>
            <div className="flex-1 h-4">
              <div className="h-full rounded" style={{ width: `${(100 * (data.overall[k] || 0)) / peak}%`, background: ROLE_COLORS[k] }} />
            </div>
            <span className="w-24 flex-shrink-0 text-[11px] text-gray-500 tabular-nums">
              {(data.overall[k] || 0).toLocaleString()} · {Math.round((100 * (data.overall[k] || 0)) / data.total)}%
            </span>
          </div>
        ))}
      </div>
      <p className="text-xs font-medium text-gray-500 mb-2">By show</p>
      <StackedShareBars rows={rows} keys={data.kinds} colors={ROLE_COLORS} labels={data.labels} />
      <p className="text-xs text-gray-400 mt-3">
        Sorted from the wording of each guest's current title, so it's approximate: "co-founder and CEO" counts
        as a founder, "VP" or "Head of" as an executive, "senior associate" as an analyst, an attorney under
        advisor / lawyer. A guest with several titles counts once, under the first kind in this order: founder,
        CEO, official, investor, academic, analyst, journalist, activist, advisor, executive, engineer.
      </p>
    </div>
  );
}
