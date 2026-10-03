import { useState } from 'react';

const W = 640;
const H = 260;
const PAD = { top: 18, right: 16, bottom: 30, left: 40 };
const INSET = 18;   // keeps the first and last points clear of the axis

// Shares per year for a set of categories. With nothing picked (the
// default) every line is drawn on one shared scale; picking a category draws
// just that line on its own scale with the values labelled, so a small one
// like government guests (3–8%) isn't a flat line along the bottom. Clicking
// it again goes back to every line. With nothing picked, hovering a legend
// entry picks its line out and hovering a year lists every value.
//   items: [{year, total, counts: {key: n}}]
//   share(item, key) -> percent
export default function ShareByYearChart({ items, keys, colors, labels, partialYear, share,
                                           describeYear, footnote }) {
  const [focus, setFocus] = useState(null);
  const [hoverYear, setHoverYear] = useState(null);
  const [hoverKey, setHoverKey] = useState(null);

  const all = focus == null;
  const shown = all ? keys : [focus];
  const series = shown.map(k => ({ key: k, values: items.map(it => share(it, k)) }));
  const top = Math.max(1, ...series.flatMap(s => s.values));
  const step = top > 40 ? 10 : top > 16 ? 5 : top > 8 ? 2 : 1;
  const peak = Math.max(step, Math.ceil((top * 1.15) / step) * step);
  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;
  const xAt = i => PAD.left + INSET + (i / Math.max(1, items.length - 1)) * (plotW - 2 * INSET);
  const yAt = v => PAD.top + plotH - (v / peak) * plotH;
  const partialIdx = items.findIndex(it => it.year === partialYear);
  const ticks = Array.from({ length: Math.floor(peak / step) + 1 }, (_, i) => i * step);
  const path = vals => vals.map((v, i) => `${i ? 'L' : 'M'}${xAt(i).toFixed(1)} ${yAt(v).toFixed(1)}`).join(' ');
  const faded = k => all && hoverKey && hoverKey !== k;

  let caption = '';
  if (hoverYear != null) {
    const it = items[hoverYear];
    caption = all
      ? `${it.year}: ` + keys.map(k => ({ k, v: share(it, k) })).sort((a, b) => b.v - a.v)
          .map(({ k, v }) => `${labels[k]} ${v.toFixed(0)}%`).join(' · ')
      : describeYear(it, focus);
  }

  return (
    <div>
      <div className="flex flex-wrap gap-1.5 mb-3" onMouseLeave={() => setHoverKey(null)}>
        {keys.map(k => (
          <button key={k} onClick={() => setFocus(f => (f === k ? null : k))} onMouseEnter={() => setHoverKey(k)}
            title={focus === k ? 'Back to every line' : `Just ${labels[k]}, on its own scale`}
            className={`flex items-center gap-1.5 text-xs rounded px-2 py-0.5 ${focus === k ? 'bg-gray-900 text-white' : 'text-gray-600 hover:bg-gray-100'}`}>
            <span className="w-2.5 h-2.5 rounded-sm flex-shrink-0" style={{ background: colors[k] }} />
            {labels[k]}
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
          {hoverYear != null && (
            <line x1={xAt(hoverYear)} x2={xAt(hoverYear)} y1={PAD.top} y2={PAD.top + plotH} stroke="#e5e7eb" />
          )}
          {!all && (
            <path d={`${path(series[0].values)} L${xAt(items.length - 1)} ${yAt(0)} L${xAt(0)} ${yAt(0)} Z`}
              fill={colors[focus]} opacity={0.13} />
          )}
          {series.map(({ key, values }) => (
            <g key={key} opacity={faded(key) ? 0.15 : 1}>
              <path d={path(values)} fill="none" stroke={colors[key]}
                strokeWidth={all ? (hoverKey === key ? 3 : 1.75) : 2.5} />
              {values.map((v, i) => (
                <g key={i}>
                  <circle cx={xAt(i)} cy={yAt(v)} r={all ? (hoverYear === i ? 3.5 : 2) : hoverYear === i ? 5 : 3.5}
                    fill={colors[key]} stroke="white" strokeWidth={all ? 0.75 : 1.5} />
                  {!all && (
                    <text x={xAt(i)} y={yAt(v) - 9} fontSize={10} fill="#374151" textAnchor="middle">
                      {v.toFixed(v < 10 ? 1 : 0)}%
                    </text>
                  )}
                </g>
              ))}
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
      <p className="text-xs text-gray-500 min-h-[1rem] mt-1">{caption}</p>
      <p className="text-xs text-gray-400 mt-1">{footnote}</p>
    </div>
  );
}
