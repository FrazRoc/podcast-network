import { useState, useEffect } from 'react';
import { API_BASE_URL } from '../config';
import { ORG_TYPE_COLORS, ORG_TYPE_LABELS } from '../chartUtils';
import StackedShareBars from './StackedShareBars';

// Who each show books: its guests split by the kind of organisation they
// work for. Only guests whose organisation has a type are counted.
export default function ShowGuestMixChart() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/show-guest-mix`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading…</p>;

  const rows = data.items.map(s => ({ id: s.podcast_id, label: s.title, counts: s.counts, total: s.typed }));
  return (
    <>
      <StackedShareBars rows={rows} keys={data.types} colors={ORG_TYPE_COLORS} labels={ORG_TYPE_LABELS} />
      <p className="text-xs text-gray-400 mt-3">
        Shows with at least 30 guests whose organisation has a known type. The number is those guests;
        guests at small, unclassified organisations aren't counted.
      </p>
    </>
  );
}
