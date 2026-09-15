import { useState, useEffect, useMemo } from 'react';
import { API_BASE_URL } from '../config';

const W = 640;
const PAD = { top: 8, right: 44, bottom: 34, left: 132 };
const ROW = 23;
const BAR = 13;

// Two steps of one hue, not two hues: the segments are parts of a single
// total, not separate categories. Stepping by lightness also survives every
// form of colour blindness, which two teals of equal lightness would not.
const OWN = '#0f766e';
const VIA = '#7dd3c8';

export default function BridgeChart() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [hover, setHover] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/bridges`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  const { people, maxX, plotW, H } = useMemo(() => {
    if (!data) return { people: [], maxX: 1, plotW: 1, H: 120 };
    // Already ordered by shows-of-their-own, descending: travellers at the top,
    // single-show hosts at the bottom. Sorting by reach instead would produce a
    // near-flat list, since everyone here is within a dozen shows of each other.
    const people = data.people;
    // Scale to the whole network, so a bar's length reads as "this share of
    // every show there is" rather than just against the leader.
    const maxX = data.total_shows;
    return {
      people,
      maxX,
      plotW: W - PAD.left - PAD.right,
      H: PAD.top + people.length * ROW + PAD.bottom,
    };
  }, [data]);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading…</p>;

  const x = v => (v / maxX) * plotW;
  const ticks = [0, 20, 40, 60, maxX].filter((v, i, a) => a.indexOf(v) === i && v <= maxX);

  return (
    <div className="overflow-x-auto">
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W }}
           onMouseLeave={() => setHover(null)}>
        {ticks.map(v => (
          <g key={v}>
            <line x1={PAD.left + x(v)} x2={PAD.left + x(v)} y1={PAD.top} y2={H - PAD.bottom}
                  stroke={v === maxX ? '#e5e7eb' : '#f3f4f6'}
                  strokeDasharray={v === maxX ? '3 3' : undefined} />
            <text x={PAD.left + x(v)} y={H - PAD.bottom + 14} fontSize={10} fill="#9ca3af"
                  textAnchor="middle">{v}</text>
          </g>
        ))}

        {people.map((p, i) => {
          const y = PAD.top + i * ROW + (ROW - BAR) / 2;
          const active = hover?.host_id === p.host_id;
          // 2px of surface between the segments so they read as two parts
          // rather than one bar with a colour change.
          const ownW = Math.max(0, x(p.own_shows) - 2);
          return (
            <g key={p.host_id}
               onMouseEnter={() => setHover(p)}
               style={{ cursor: 'pointer' }}>
              <rect x={0} y={PAD.top + i * ROW} width={W} height={ROW}
                    fill={active ? '#f8fafc' : 'transparent'} />
              <text x={PAD.left - 8} y={y + BAR / 2 + 4} fontSize={11}
                    fill={active ? '#111827' : '#374151'} textAnchor="end">
                {p.name}
              </text>
              <rect x={PAD.left} y={y} width={ownW} height={BAR} rx={2} fill={OWN} />
              <rect x={PAD.left + x(p.own_shows)} y={y}
                    width={Math.max(0, x(p.via_others))} height={BAR} rx={2} fill={VIA} />
              <text x={PAD.left + x(p.reached) + 6} y={y + BAR / 2 + 4}
                    fontSize={10} fill="#6b7280">{p.reached}</text>
            </g>
          );
        })}

        <text x={PAD.left + plotW / 2} y={H - 4} fontSize={11} fill="#6b7280" textAnchor="middle">
          shows reachable
        </text>

        {hover && (() => {
          const i = people.findIndex(p => p.host_id === hover.host_id);
          const boxW = 208, boxH = 50;
          const ty = Math.min(Math.max(PAD.top + i * ROW - boxH - 4, 2), H - boxH - 2);
          const tx = Math.min(PAD.left + x(hover.reached) + 10, W - boxW - 4);
          return (
            <g transform={`translate(${tx}, ${ty})`} pointerEvents="none">
              <rect width={boxW} height={boxH} rx={6} fill="white" stroke="#e5e7eb" />
              <text x={8} y={17} fontSize={11} fontWeight={600} fill="#111827">{hover.name}</text>
              <text x={8} y={33} fontSize={10} fill="#6b7280">
                on {hover.own_shows} show{hover.own_shows === 1 ? '' : 's'},
                {' '}{hover.via_others} more through guests
              </text>
              <text x={8} y={45} fontSize={10} fill="#6b7280">
                {hover.reached} of {maxX} shows, via {hover.people_sat_with} people
              </text>
            </g>
          );
        })()}
      </svg>

      <div className="flex flex-wrap items-center gap-4 mt-3 text-xs text-gray-600">
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded-sm" style={{ background: OWN }} />
          shows they appear on
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded-sm" style={{ background: VIA }} />
          reached only through the people they sat with
        </span>
      </div>

      <p className="text-xs text-gray-500 mt-2">
        Out of {data.total_shows} shows in the network.
      </p>
    </div>
  );
}
