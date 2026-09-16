import { useState, useEffect, useMemo } from 'react';
import { API_BASE_URL } from '../config';

const W = 150;
const H = 42;
const PAD = 4;
const LINE = '#0d9488';
const FILL = 'rgba(13, 148, 136, 0.12)';

export default function GuestMomentumGrid() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [hover, setHover] = useState(null);   // { host_id, i }

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/guest-momentum`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  const { guests, years, peak } = useMemo(() => {
    if (!data) return { guests: [], years: [], peak: 1 };
    // One shared vertical scale across every card. Scaling each to its own
    // maximum would make a guest with three appearances look identical to one
    // with twenty-two, which is the comparison the grid exists to support.
    const peak = Math.max(1, ...data.guests.flatMap(g => g.counts));
    return { guests: data.guests, years: data.years, peak };
  }, [data]);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading…</p>;

  const plotW = W - PAD * 2;
  const plotH = H - PAD * 2;
  const xAt = i => PAD + (years.length === 1 ? plotW / 2 : (i / (years.length - 1)) * plotW);
  const yAt = v => PAD + plotH - (v / peak) * plotH;
  const partialIdx = years.indexOf(data.partial_year);

  return (
    <div>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-x-4 gap-y-5">
        {guests.map(g => {
          // Start at the first year they appeared. Years before that were
          // drawn as a flat run along the baseline, which at this height is
          // nearly indistinguishable from a year with one appearance — so a
          // debut read as a slight rise rather than a beginning. The x
          // positions stay on the shared year axis, so the cards still line up.
          const from = g.counts.findIndex(v => v > 0);
          const start = from === -1 ? 0 : from;
          const pts = g.counts
            .map((v, i) => [xAt(i), yAt(v), i])
            .slice(start);
          const line = pts
            .map(([x, y], i) => `${i ? 'L' : 'M'}${x.toFixed(1)} ${y.toFixed(1)}`)
            .join(' ');
          const area = pts.length > 1
            ? `${line} L${xAt(years.length - 1)} ${PAD + plotH} L${xAt(start)} ${PAD + plotH} Z`
            : '';
          const on = hover?.host_id === g.host_id;
          const shown = on ? hover.i : g.counts.length - 1;
          return (
            <div key={g.host_id}>
              <p className="text-xs font-medium text-gray-800 truncate" title={g.name}>{g.name}</p>
              <p className="text-[11px] text-gray-500 mb-1 h-4">
                {on
                  ? `${years[shown]}: ${g.counts[shown]}`
                  : g.earlier === 0
                    ? 'first appeared this year'
                    : `${g.recent} in 18 months`}
              </p>
              <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W }}
                   onMouseLeave={() => setHover(null)}>
                {area && <path d={area} fill={FILL} />}
                {pts.length > 1 && (
                  <path d={line} fill="none" stroke={LINE} strokeWidth={1.6}
                        strokeLinejoin="round" strokeLinecap="round" />
                )}
                {/* The final year is only partly published, so its fall is an
                    artefact for everyone. Marked hollow rather than corrected:
                    projecting it would invent episodes that do not exist. */}
                {pts.map(([x, y, i]) => (
                  <circle key={i} cx={x} cy={y} r={on && hover.i === i ? 3 : 1.7}
                          fill={i === partialIdx ? '#fff' : LINE}
                          stroke={LINE} strokeWidth={i === partialIdx ? 1.4 : 0} />
                ))}
                {/* Generous invisible hit areas — the points are 1.7px wide. */}
                {pts.map(([x, , i]) => (
                  <rect key={`h${i}`} x={x - plotW / (years.length * 2)} y={0}
                        width={plotW / years.length} height={H} fill="transparent"
                        onMouseEnter={() => setHover({ host_id: g.host_id, i })} />
                ))}
              </svg>
            </div>
          );
        })}
      </div>

      <p className="text-xs text-gray-500 mt-4">
        {years[0]}–{years[years.length - 1]}, on a shared scale. The hollow
        final point is {data.partial_year}, a year still in progress.
      </p>
    </div>
  );
}
