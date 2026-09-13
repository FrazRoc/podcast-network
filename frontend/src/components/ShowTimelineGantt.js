import { useState, useEffect, useMemo } from 'react';
import { API_BASE_URL } from '../config';
import { formatDateOnly } from '../adminUtils';

const ROW_H = 16;
const LABEL_W = 200;
const CHART_W = 600;
const ACTIVE_WINDOW_DAYS = 90;
const CHART_START = new Date(2019, 0, 1); // a handful of episodes predate this; not worth the noise

const parseDate = (s) => {
  const [y, m, d] = s.split('-').map(Number);
  return new Date(y, m - 1, d);
};

export default function ShowTimelineGantt() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [hoverIdx, setHoverIdx] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/show-timeline`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  const { shows, minDate, maxDate } = useMemo(() => {
    if (!data) return { shows: [], minDate: null, maxDate: null };
    const shows = data.shows
      .map(s => ({
        ...s,
        earliest: parseDate(s.earliest_episode_date),
        latest: parseDate(s.latest_episode_date),
        dates: s.episode_dates.map(parseDate).filter(d => d >= CHART_START),
      }))
      .filter(s => s.latest >= CHART_START) // a show entirely before the cutoff has nothing to show
      .map(s => ({ ...s, earliest: s.earliest < CHART_START ? CHART_START : s.earliest }));
    const max = new Date(); // scale to today, not just the latest episode, so dormancy is visible
    return { shows, minDate: CHART_START, maxDate: max };
  }, [data]);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading...</p>;

  const totalSpanMs = maxDate - minDate;
  const xFor = (d) => LABEL_W + ((d - minDate) / totalSpanMs) * CHART_W;
  const height = shows.length * ROW_H + 30;
  const totalWidth = LABEL_W + CHART_W;

  const now = new Date();
  const isActive = (s) => (now - s.latest) / (1000 * 60 * 60 * 24) <= ACTIVE_WINDOW_DAYS;

  // Year gridlines
  const years = [];
  for (let y = minDate.getFullYear(); y <= maxDate.getFullYear(); y++) {
    const jan1 = new Date(y, 0, 1);
    if (jan1 >= minDate && jan1 <= maxDate) years.push(y);
  }

  return (
    <div className="overflow-x-auto">
      <svg viewBox={`0 0 ${totalWidth} ${height}`} width={totalWidth} style={{ minWidth: totalWidth }}>
        {/* Year gridlines */}
        {years.map(y => (
          <g key={y}>
            <line x1={xFor(new Date(y, 0, 1))} x2={xFor(new Date(y, 0, 1))} y1={0} y2={height - 20} stroke="#f3f4f6" strokeWidth={1} />
            <text x={xFor(new Date(y, 0, 1))} y={height - 6} fontSize={10} fill="#9ca3af" textAnchor="middle">{y}</text>
          </g>
        ))}

        {shows.map((s, i) => {
          const y = i * ROW_H + ROW_H / 2;
          const active = isActive(s);
          const barColor = active ? '#0d9488' : '#9ca3af';
          const hovered = hoverIdx === i;
          return (
            <g key={s.podcast_id}
              onMouseEnter={() => setHoverIdx(i)}
              onMouseLeave={() => setHoverIdx(null)}
              style={{ cursor: 'pointer' }}
            >
              {/* Full-width hit area */}
              <rect x={0} y={i * ROW_H} width={totalWidth} height={ROW_H} fill={hovered ? '#f0fdfa' : 'transparent'} />
              <text x={LABEL_W - 8} y={y + 3} fontSize={10} fill="#374151" textAnchor="end"
                style={{ maxWidth: LABEL_W }}>
                {s.title.length > 26 ? s.title.slice(0, 25) + '…' : s.title}
              </text>
              <line x1={xFor(s.earliest)} x2={xFor(s.latest)} y1={y} y2={y} stroke={barColor} strokeWidth={hovered ? 3 : 2} />
              {s.dates.map((d, di) => (
                <circle key={di} cx={xFor(d)} cy={y} r={hovered ? 2 : 1.3} fill={barColor} />
              ))}
            </g>
          );
        })}

        {/* Tooltip */}
        {hoverIdx !== null && shows[hoverIdx] && (() => {
          const s = shows[hoverIdx];
          const tx = Math.min(xFor(s.latest) + 10, totalWidth - 220);
          const ty = Math.max(hoverIdx * ROW_H - 10, 0);
          return (
            <g transform={`translate(${tx}, ${ty})`}>
              <rect width={210} height={48} rx={6} fill="white" stroke="#e5e7eb" />
              <text x={8} y={16} fontSize={11} fontWeight={600} fill="#111827">{s.title}</text>
              <text x={8} y={31} fontSize={10} fill="#6b7280">
                {formatDateOnly(s.earliest_episode_date)} – {formatDateOnly(s.latest_episode_date)}
              </text>
              <text x={8} y={43} fontSize={10} fill="#6b7280">
                {s.dates.length} episode{s.dates.length !== 1 ? 's' : ''} · {isActive(s) ? 'Active' : 'Dormant'}
              </text>
            </g>
          );
        })()}
      </svg>

      <div className="flex items-center gap-4 mt-3 text-xs text-gray-500">
        <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 bg-teal-600 inline-block" /> Active (episode in last {ACTIVE_WINDOW_DAYS} days)</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 bg-gray-400 inline-block" /> Dormant</span>
      </div>
    </div>
  );
}
