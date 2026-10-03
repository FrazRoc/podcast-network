import { useState, useEffect } from 'react';
import { API_BASE_URL } from '../config';
import { ORG_TYPE_COLORS, ORG_TYPE_LABELS } from '../chartUtils';
import ShareByYearChart from './ShareByYearChart';

// Share of guest appearances from each kind of organisation, per year. All
// kinds together by default; pick one to see it on its own scale (government
// is the one that moves).
export default function GuestMixByYearChart() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/guest-mix-by-year`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading…</p>;

  return (
    <ShareByYearChart items={data.items} keys={data.types} colors={ORG_TYPE_COLORS} labels={ORG_TYPE_LABELS}
      partialYear={data.partial_year}
      share={(it, t) => (100 * (it.counts[t] || 0)) / (it.total || 1)}
      describeYear={(it, t) => `${it.year}: ${it.counts[t] || 0} of ${it.total} guest appearances were from ${ORG_TYPE_LABELS[t].toLowerCase()} organisations.`}
      footnote="Each guest appearance counted under the organisation the guest worked for at the time, among guests whose organisation has a known type. * part year." />
  );
}
