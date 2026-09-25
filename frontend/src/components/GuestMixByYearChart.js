import { useState, useEffect } from 'react';
import { API_BASE_URL } from '../config';
import { ORG_TYPE_COLORS, ORG_TYPE_LABELS } from '../chartUtils';

const W = 640;
const H = 260;
const PAD = { top: 18, right: 16, bottom: 30, left: 40 };

// Share of guest appearances from one kind of organisation, per year, on
// its own scale — on a shared 0–40% axis the government line (3–8%) is flat
// along the bottom and its 2025 drop disappears. Government first, since
// it's the one that moves; the buttons switch to any other kind.
export default function GuestMixByYearChart() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [focus, setFocus] = useState('government');
  const [hoverYear, setHoverYear] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/guest-mix-by-year`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading…</p>;

  const items = data.items;
  const share = (it, t) => (100 * (it.counts[t] || 0)) / (it.total || 1);
  const values = items.map(it => share(it, focus));
  const colour = ORG_TYPE_COLORS[focus];
  const step = Math.max(...values) > 20 ? 10 : Math.max(...values) > 8 ? 2 : 1;
  const peak = Math.max(step, Math.ceil((Math.max(...values) * 1.15) / step) * step);
  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;
  const INSET = 18;   // keeps the first and last value labels clear of the axis
  const xAt = i => PAD.left + INSET + (i / Math.max(1, items.length - 1)) * (plotW - 2 * INSET);
  const yAt = v => PAD.top + plotH - (v / peak) * plotH;
  const partialIdx = items.findIndex(it => it.year === data.partial_year);
  const line = values.map((v, i) => `${i ? 'L' : 'M'}${xAt(i).toFixed(1)} ${yAt(v).toFixed(1)}`).join(' ');
  const area = `${line} L${xAt(items.length - 1)} ${yAt(0)} L${xAt(0)} ${yAt(0)} Z`;
  const ticks = Array.from({ length: Math.floor(peak / step) + 1 }, (_, i) => i * step);

  return (
    <div>
      <div className="flex flex-wrap gap-1.5 mb-3">
        {data.types.map(t => (
          <button key={t} onClick={() => setFocus(t)}
            className={`flex items-center gap-1.5 text-xs rounded px-2 py-0.5 ${focus === t ? 'bg-gray-900 text-white' : 'text-gray-600 hover:bg-gray-100'}`}>
            <span className="w-2.5 h-2.5 rounded-sm" style={{ background: ORG_TYPE_COLORS[t] }} />
            {ORG_TYPE_LABELS[t]}
          </button>
        ))}
      </div>
      <div className="overflow-x-auto">
        <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W }} onMouseLeave={() => setHoverYear(null)}>
          {ticks.map(v => (
            <g key={v}>
              <line x1={PAD.left} x2={PAD.left + plotW} y1={yAt(v)} y2={yAt(v)} stroke="#f3f4f6" />
              <text x={PAD.left - 6} y={yAt(v) + 3} fontSize={10} fill="#9ca3af" textAnchor="end">{v}%</text>
            </g>
          ))}
          <path d={area} fill={colour} opacity={0.12} />
          <path d={line} fill="none" stroke={colour} strokeWidth={2.5} />
          {values.map((v, i) => (
            <g key={i}>
              <circle cx={xAt(i)} cy={yAt(v)} r={hoverYear === i ? 5 : 3.5} fill={colour}
                stroke="white" strokeWidth={1.5} strokeDasharray={i === partialIdx ? '2 1.5' : undefined} />
              <text x={xAt(i)} y={yAt(v) - 9} fontSize={10} fill="#374151" textAnchor="middle">{v.toFixed(1)}%</text>
            </g>
          ))}
          {items.map((it, i) => (
            <g key={it.year}>
              <text x={xAt(i)} y={H - PAD.bottom + 16} fontSize={10} fill="#9ca3af" textAnchor="middle">
                {it.year}{i === partialIdx ? '*' : ''}
              </text>
              <rect x={xAt(i) - plotW / (items.length * 2)} y={PAD.top} width={plotW / items.length} height={plotH}
                fill="transparent" onMouseEnter={() => setHoverYear(i)} />
            </g>
          ))}
        </svg>
      </div>
      <p className="text-xs text-gray-500 h-4 mt-1">
        {hoverYear != null && `${items[hoverYear].year}: ${items[hoverYear].counts[focus] || 0} of ${items[hoverYear].total} guest appearances were from ${ORG_TYPE_LABELS[focus].toLowerCase()} organisations.`}
      </p>
      <p className="text-xs text-gray-400 mt-1">
        Each guest appearance counted under the organisation the guest worked for at the time, among guests whose
        organisation has a known type. * part year.
      </p>
    </div>
  );
}
