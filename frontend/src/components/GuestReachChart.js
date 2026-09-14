import { useState, useEffect, useMemo } from 'react';
import { API_BASE_URL } from '../config';

const W = 640;
const H = 420;
const PAD = { top: 16, right: 20, bottom: 44, left: 52 };

// Appearances span 2 to ~100 while most guests sit near the bottom, so a linear
// axis would press almost everyone onto one line. sqrt keeps the crowded low
// end readable without the distortion of a log scale on small counts.
const sq = (v) => Math.sqrt(v);

export default function GuestReachChart() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [hover, setHover] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/guest-reach`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  const { points, maxShows, maxApp, labelled } = useMemo(() => {
    if (!data) return { points: [], maxShows: 1, maxApp: 1, labelled: [] };
    const guests = data.guests;
    const maxShows = Math.max(...guests.map(g => g.shows));
    const maxApp = Math.max(...guests.map(g => g.appearances));
    const plotW = W - PAD.left - PAD.right;
    const plotH = H - PAD.top - PAD.bottom;

    const points = guests.map(g => ({
      ...g,
      x: PAD.left + (sq(g.shows) / sq(maxShows)) * plotW,
      y: PAD.top + plotH - (sq(g.appearances) / sq(maxApp)) * plotH,
      perShow: g.appearances / g.shows,
    }));

    // Label only the extremes of each behaviour: the widest reach, and the
    // most concentrated. Labelling every point would be unreadable.
    const byShows = [...points].sort((a, b) => b.shows - a.shows).slice(0, 5);
    const byLoyalty = [...points]
      .filter(p => p.appearances >= 12)
      .sort((a, b) => b.perShow - a.perShow)
      .slice(0, 4);
    const seen = new Set();
    const labelled = [...byShows, ...byLoyalty].filter(p => {
      if (seen.has(p.host_id)) return false;
      seen.add(p.host_id);
      return true;
    });
    return { points, maxShows, maxApp, labelled };
  }, [data]);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading…</p>;

  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;
  const xTicks = [1, 2, 4, 8, 16, 32].filter(v => v <= maxShows);
  const yTicks = [2, 5, 10, 25, 50, 100].filter(v => v <= maxApp);
  const xFor = (v) => PAD.left + (sq(v) / sq(maxShows)) * plotW;
  const yFor = (v) => PAD.top + plotH - (sq(v) / sq(maxApp)) * plotH;

  return (
    <div className="overflow-x-auto">
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W }}
           onMouseLeave={() => setHover(null)}>
        {yTicks.map(v => (
          <g key={`y${v}`}>
            <line x1={PAD.left} x2={W - PAD.right} y1={yFor(v)} y2={yFor(v)} stroke="#f3f4f6" />
            <text x={PAD.left - 8} y={yFor(v) + 3} fontSize={10} fill="#9ca3af" textAnchor="end">{v}</text>
          </g>
        ))}
        {xTicks.map(v => (
          <g key={`x${v}`}>
            <line x1={xFor(v)} x2={xFor(v)} y1={PAD.top} y2={H - PAD.bottom} stroke="#f3f4f6" />
            <text x={xFor(v)} y={H - PAD.bottom + 14} fontSize={10} fill="#9ca3af" textAnchor="middle">{v}</text>
          </g>
        ))}

        {/* One appearance per show: everyone below this line visited each show
            once, everyone above returned. */}
        <line x1={xFor(1)} y1={yFor(1)} x2={xFor(maxShows)} y2={yFor(maxShows)}
              stroke="#e5e7eb" strokeDasharray="3 3" />
        <text x={xFor(maxShows) - 4} y={yFor(maxShows) - 6} fontSize={9} fill="#cbd5e1" textAnchor="end">
          one visit per show
        </text>

        {points.map(p => {
          const active = hover?.host_id === p.host_id;
          return (
            <circle
              key={p.host_id}
              cx={p.x} cy={p.y}
              r={active ? 6 : 3.5}
              fill="#0d9488"
              fillOpacity={active ? 1 : 0.35}
              stroke={active ? '#fff' : 'none'}
              strokeWidth={2}
              onMouseEnter={() => setHover(p)}
              style={{ cursor: 'pointer' }}
            />
          );
        })}

        {labelled.map((p, i) => {
          // Points near the right edge get their label on the other side, and
          // two guests with identical counts land on the same pixel — Bill
          // Meehan and Pat Hohl both sit at 16 appearances on 1 show — so the
          // second of a colliding pair is nudged clear.
          const flip = p.x > W * 0.7;
          const collides = labelled
            .slice(0, i)
            .some(q => Math.abs(q.x - p.x) < 4 && Math.abs(q.y - p.y) < 4);
          return (
            <text
              key={`l${p.host_id}`}
              x={flip ? p.x - 8 : p.x + 8}
              y={p.y + 3 + (collides ? 12 : 0)}
              fontSize={10}
              fill="#374151"
              textAnchor={flip ? 'end' : 'start'}
              pointerEvents="none"
            >
              {p.name}
            </text>
          );
        })}

        {hover && (() => {
          const boxW = 190, boxH = 46;
          const tx = Math.min(Math.max(hover.x + 12, PAD.left), W - boxW - 4);
          const ty = Math.max(hover.y - boxH - 8, 2);
          return (
            <g transform={`translate(${tx}, ${ty})`} pointerEvents="none">
              <rect width={boxW} height={boxH} rx={6} fill="white" stroke="#e5e7eb" />
              <text x={8} y={17} fontSize={11} fontWeight={600} fill="#111827">{hover.name}</text>
              <text x={8} y={33} fontSize={10} fill="#6b7280">
                {hover.appearances} appearances across {hover.shows} show{hover.shows === 1 ? '' : 's'}
              </text>
            </g>
          );
        })()}

        <text x={PAD.left + plotW / 2} y={H - 6} fontSize={11} fill="#6b7280" textAnchor="middle">
          shows appeared on
        </text>
        <text x={14} y={PAD.top + plotH / 2} fontSize={11} fill="#6b7280" textAnchor="middle"
              transform={`rotate(-90 14 ${PAD.top + plotH / 2})`}>
          guest appearances
        </text>
      </svg>

      <p className="text-xs text-gray-500 mt-2">
        {data.guests.length.toLocaleString()} guests with more than one appearance.
        A further {data.single_appearance_guests.toLocaleString()} appeared exactly once and
        would all share a single point.
      </p>
    </div>
  );
}
