import { useState, useEffect, useMemo } from 'react';
import { API_BASE_URL } from '../config';

const W = 640;
const H = 380;
const PAD = { top: 12, right: 92, bottom: 38, left: 46 };

// One hue, oldest cohort darkest. Debut year is an ordered dimension, so it
// takes a sequential ramp rather than a categorical set — the bands are not
// separate things, they are the same thing at different ages.
const RAMP = ['#134e4a', '#115e59', '#0f766e', '#0d9488', '#14b8a6', '#2dd4bf', '#5eead4', '#99f6e4'];

export default function GuestCohortChart() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [hover, setHover] = useState(null);   // { i, cohort }

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/guest-cohorts`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  const { years, bands, peak } = useMemo(() => {
    if (!data) return { years: [], bands: [], peak: 1 };
    // Stack oldest at the bottom so each year's newcomers arrive on top and
    // the persistent base is the part that never moves.
    let below = new Array(data.years.length).fill(0);
    const bands = data.cohorts.map((c, ci) => {
      const lower = below;
      const upper = c.counts.map((n, i) => lower[i] + n);
      below = upper;
      return { cohort: c.cohort, counts: c.counts, lower, upper, colour: RAMP[ci % RAMP.length] };
    });
    return { years: data.years, bands, peak: Math.max(1, ...data.totals) };
  }, [data]);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading…</p>;

  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;
  const xAt = i => PAD.left + (i / (years.length - 1)) * plotW;
  const yAt = v => PAD.top + plotH - (v / peak) * plotH;
  const partialIdx = years.indexOf(data.partial_year);

  const areaFor = b => {
    const up = b.upper.map((v, i) => `${i ? 'L' : 'M'}${xAt(i).toFixed(1)} ${yAt(v).toFixed(1)}`);
    const down = b.lower
      .map((v, i) => [xAt(i), yAt(v)])
      .reverse()
      .map(([x, y]) => `L${x.toFixed(1)} ${y.toFixed(1)}`);
    return `${up.join(' ')} ${down.join(' ')} Z`;
  };

  const ticks = [0, 250, 500, 750, 1000].filter(v => v <= peak * 1.05);

  return (
    <div className="overflow-x-auto">
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W }}
           onMouseLeave={() => setHover(null)}>
        {ticks.map(v => (
          <g key={v}>
            <line x1={PAD.left} x2={PAD.left + plotW} y1={yAt(v)} y2={yAt(v)} stroke="#f3f4f6" />
            <text x={PAD.left - 8} y={yAt(v) + 3} fontSize={10} fill="#9ca3af" textAnchor="end">{v}</text>
          </g>
        ))}

        {bands.map(b => (
          <path key={b.cohort} d={areaFor(b)} fill={b.colour}
                stroke="#fff" strokeWidth={1}
                opacity={hover && hover.cohort !== b.cohort ? 0.35 : 1}
                onMouseEnter={() => setHover(h => ({ i: h?.i ?? years.length - 1, cohort: b.cohort }))} />
        ))}

        {/* The final year is part-published, so the drop is an artefact. */}
        <line x1={xAt(partialIdx)} x2={xAt(partialIdx)} y1={PAD.top} y2={PAD.top + plotH}
              stroke="#94a3b8" strokeDasharray="3 3" />

        {years.map((y, i) => (
          <g key={y}>
            <text x={xAt(i)} y={H - PAD.bottom + 14} fontSize={10}
                  fill={i === partialIdx ? '#64748b' : '#9ca3af'} textAnchor="middle">{y}</text>
            <rect x={xAt(i) - plotW / (years.length * 2)} y={PAD.top}
                  width={plotW / years.length} height={plotH} fill="transparent"
                  onMouseEnter={() => setHover(h => ({ i, cohort: h?.cohort ?? null }))} />
          </g>
        ))}
        <text x={xAt(partialIdx)} y={H - 4} fontSize={9} fill="#94a3b8" textAnchor="middle">
          part year
        </text>

        {/* Each band labelled at the right edge where it is thickest enough */}
        {bands.map(b => {
          const last = years.length - 1;
          const mid = (b.lower[last] + b.upper[last]) / 2;
          const tall = yAt(b.lower[last]) - yAt(b.upper[last]);
          if (tall < 9) return null;
          return (
            <text key={`l${b.cohort}`} x={PAD.left + plotW + 6} y={yAt(mid) + 3}
                  fontSize={10} fill="#6b7280">
              {b.cohort}{b.cohort === years[0] ? ' & before' : ''}
            </text>
          );
        })}

        {hover && (() => {
          const b = hover.cohort != null ? bands.find(x => x.cohort === hover.cohort) : null;
          const boxW = 196, boxH = b ? 50 : 34;
          const tx = Math.min(Math.max(xAt(hover.i) - boxW / 2, 2), W - boxW - 2);
          return (
            <g transform={`translate(${tx}, 2)`} pointerEvents="none">
              <rect width={boxW} height={boxH} rx={6} fill="white" stroke="#e5e7eb" />
              <text x={8} y={16} fontSize={11} fontWeight={600} fill="#111827">
                {years[hover.i]} — {data.totals[hover.i]} guests
              </text>
              {b && (
                <>
                  <text x={8} y={31} fontSize={10} fill="#6b7280">
                    {b.counts[hover.i]} of them first appeared
                  </text>
                  <text x={8} y={43} fontSize={10} fill="#6b7280">
                    in {b.cohort}{b.cohort === years[0] ? ' or earlier' : ''}
                  </text>
                </>
              )}
            </g>
          );
        })()}
      </svg>
    </div>
  );
}
