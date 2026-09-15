import { useState, useEffect, useMemo } from 'react';
import { API_BASE_URL } from '../config';
import { formatDateOnly } from '../adminUtils';

const ROW_H = 15;
const LABEL_W = 168;
const CHART_W = 620;
const ACTIVE_WINDOW_DAYS = 90;
const CELL_GAP = 0.6;

// One hue, light to dark: the cells encode a magnitude — episodes that month —
// not a category. Empty months are left as bare surface rather than given a
// colour of their own, so a gap in publishing reads as a gap.
const SHADES = ['#ccfbf1', '#5eead4', '#14b8a6', '#0d9488', '#115e59'];
const DORMANT = '#d4d4d8';

// Banded rather than a continuous ramp. The distinction worth seeing is weekly
// against fortnightly against monthly, and a linear scale would stretch itself
// to fit the rare 30-episode month, flattening everything the rest of the
// network actually does.
const shadeFor = (n) => (n >= 9 ? 4 : n >= 5 ? 3 : n >= 3 ? 2 : n >= 2 ? 1 : 0);

const parseDate = (s) => {
  const [y, m, d] = s.split('-').map(Number);
  return new Date(y, m - 1, d);
};

export default function ShowTimelineGantt() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [hover, setHover] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/show-timeline`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  const { shows, months, cellW } = useMemo(() => {
    if (!data?.months?.length) return { shows: [], months: [], cellW: 1 };
    const now = new Date();
    const shows = data.shows.map(s => {
      const monthly = s.monthly || [];
      return {
        ...s,
        monthly,
        peak: monthly.reduce((a, b) => Math.max(a, b), 0),
        total: monthly.reduce((a, b) => a + b, 0),
        active: (now - parseDate(s.latest_episode_date)) / 86400000 <= ACTIVE_WINDOW_DAYS,
      };
    });
    return { shows, months: data.months, cellW: CHART_W / data.months.length };
  }, [data]);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading…</p>;

  const height = shows.length * ROW_H + 26;
  const totalW = LABEL_W + CHART_W;
  const yearTicks = months.map((m, i) => ({ m, i })).filter(({ m }) => m.endsWith('-01'));

  return (
    <div className="overflow-x-auto">
      <svg viewBox={`0 0 ${totalW} ${height}`} width="100%" style={{ minWidth: 560 }}
           onMouseLeave={() => setHover(null)}>
        {yearTicks.map(({ m, i }) => (
          <g key={m}>
            <line x1={LABEL_W + i * cellW} x2={LABEL_W + i * cellW}
                  y1={0} y2={height - 18} stroke="#f1f5f9" />
            <text x={LABEL_W + i * cellW} y={height - 5} fontSize={10} fill="#9ca3af"
                  textAnchor="middle">{m.slice(0, 4)}</text>
          </g>
        ))}

        {shows.map((s, row) => {
          const y = row * ROW_H;
          const on = hover?.podcast_id === s.podcast_id;
          return (
            <g key={s.podcast_id} style={{ cursor: 'pointer' }}>
              <rect x={0} y={y} width={totalW} height={ROW_H}
                    fill={on ? '#f0fdfa' : 'transparent'}
                    onMouseEnter={() => setHover({ ...s, month: null })} />
              <text x={LABEL_W - 8} y={y + ROW_H - 4} fontSize={10}
                    fill={on ? '#111827' : '#374151'} textAnchor="end">
                {s.title.length > 24 ? s.title.slice(0, 23) + '…' : s.title}
              </text>
              {s.monthly.map((n, i) => n > 0 && (
                <rect key={i}
                      x={LABEL_W + i * cellW} y={y + 2.5}
                      width={Math.max(cellW - CELL_GAP, 0.8)} height={ROW_H - 5}
                      fill={s.active ? SHADES[shadeFor(n)] : DORMANT}
                      onMouseEnter={() => setHover({ ...s, month: months[i], count: n })} />
              ))}
            </g>
          );
        })}

        {hover && (() => {
          const row = shows.findIndex(s => s.podcast_id === hover.podcast_id);
          const boxW = 232, boxH = hover.month ? 62 : 48;
          const tx = Math.min(LABEL_W + 10, totalW - boxW - 4);
          const ty = Math.min(Math.max(row * ROW_H - boxH - 2, 2), height - boxH - 20);
          return (
            <g transform={`translate(${tx}, ${ty})`} pointerEvents="none">
              <rect width={boxW} height={boxH} rx={6} fill="white" stroke="#e5e7eb" />
              <text x={8} y={17} fontSize={11} fontWeight={600} fill="#111827">{hover.title}</text>
              <text x={8} y={32} fontSize={10} fill="#6b7280">
                {formatDateOnly(hover.earliest_episode_date)} – {formatDateOnly(hover.latest_episode_date)}
              </text>
              <text x={8} y={44} fontSize={10} fill="#6b7280">
                {hover.total} episodes · peak {hover.peak}/month · {hover.active ? 'active' : 'dormant'}
              </text>
              {hover.month && (
                <text x={8} y={56} fontSize={10} fill="#0f766e">
                  {hover.month}: {hover.count} episode{hover.count === 1 ? '' : 's'}
                </text>
              )}
            </g>
          );
        })()}
      </svg>

      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 mt-3 text-xs text-gray-500">
        <span className="flex items-center gap-1.5">
          episodes per month
          {SHADES.map((c, i) => (
            <span key={i} className="inline-block w-4 h-3 rounded-sm" style={{ background: c }} />
          ))}
        </span>
        <span className="text-gray-400">1 · 2 · 3–4 · 5–8 · 9+</span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-4 h-3 rounded-sm" style={{ background: DORMANT }} />
          dormant (nothing in {ACTIVE_WINDOW_DAYS} days)
        </span>
      </div>
    </div>
  );
}
