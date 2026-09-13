import { useState, useEffect, useRef } from 'react';
import { API_BASE_URL } from '../config';

const WIDTH = 800;
const HEIGHT = 320;
const PAD = { top: 20, right: 20, bottom: 36, left: 48 };
const PLOT_W = WIDTH - PAD.left - PAD.right;
const PLOT_H = HEIGHT - PAD.top - PAD.bottom;

export default function GuestAppearanceChart() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [hoverIdx, setHoverIdx] = useState(null);
  const svgRef = useRef(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/guest-appearances?limit=200`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading...</p>;

  const items = data.items;
  const yMax = items[0]?.appearances || 1;
  const n = items.length;

  const xFor = (i) => PAD.left + (n <= 1 ? 0 : (i / (n - 1)) * PLOT_W);
  const yFor = (v) => PAD.top + PLOT_H - (v / yMax) * PLOT_H;

  const linePath = items.map((d, i) => `${i === 0 ? 'M' : 'L'} ${xFor(i)} ${yFor(d.appearances)}`).join(' ');
  const areaPath = `${linePath} L ${xFor(n - 1)} ${PAD.top + PLOT_H} L ${xFor(0)} ${PAD.top + PLOT_H} Z`;

  const handleMouseMove = (e) => {
    const rect = svgRef.current.getBoundingClientRect();
    const x = ((e.clientX - rect.left) / rect.width) * WIDTH;
    const frac = Math.max(0, Math.min(1, (x - PAD.left) / PLOT_W));
    const idx = Math.round(frac * (n - 1));
    setHoverIdx(idx);
  };

  const yTicks = [0, 0.25, 0.5, 0.75, 1].map(t => Math.round(t * yMax));
  const topFive = items.slice(0, 5);

  return (
    <div>
      <div className="mb-3 text-sm text-gray-600">
        <span className="font-semibold text-gray-900">{data.total_people}</span> people have appeared across all shows.
        The top 5: {topFive.map((p, i) => (
          <span key={p.host_id}>
            {i > 0 && ', '}
            <a href={`/admin/people?host_id=${p.host_id}`} className="text-teal-600 hover:underline">
              {p.name}
            </a> ({p.appearances})
          </span>
        ))}
      </div>

      <svg
        ref={svgRef}
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        className="w-full h-auto"
        onMouseMove={handleMouseMove}
        onMouseLeave={() => setHoverIdx(null)}
      >
        {/* Recessive gridlines + y-axis labels */}
        {yTicks.map(v => (
          <g key={v}>
            <line x1={PAD.left} x2={WIDTH - PAD.right} y1={yFor(v)} y2={yFor(v)} stroke="#f3f4f6" strokeWidth={1} />
            <text x={PAD.left - 8} y={yFor(v) + 4} textAnchor="end" fontSize={11} fill="#9ca3af">{v}</text>
          </g>
        ))}

        {/* Area + line */}
        <path d={areaPath} fill="#0d948820" />
        <path d={linePath} fill="none" stroke="#0d9488" strokeWidth={2} />

        {/* X-axis label */}
        <text x={PAD.left} y={HEIGHT - 8} fontSize={11} fill="#9ca3af">Most appearances</text>
        <text x={WIDTH - PAD.right} y={HEIGHT - 8} textAnchor="end" fontSize={11} fill="#9ca3af">Fewest (rank {n})</text>

        {/* Hover crosshair + tooltip */}
        {hoverIdx !== null && items[hoverIdx] && (
          <>
            <line
              x1={xFor(hoverIdx)} x2={xFor(hoverIdx)}
              y1={PAD.top} y2={PAD.top + PLOT_H}
              stroke="#0d9488" strokeWidth={1} strokeDasharray="3,3"
            />
            <circle cx={xFor(hoverIdx)} cy={yFor(items[hoverIdx].appearances)} r={4} fill="#0d9488" />
            <g transform={`translate(${Math.min(xFor(hoverIdx) + 10, WIDTH - 170)}, ${Math.max(yFor(items[hoverIdx].appearances) - 40, PAD.top)})`}>
              <rect width={160} height={36} rx={6} fill="white" stroke="#e5e7eb" />
              <text x={8} y={15} fontSize={12} fontWeight={600} fill="#111827">
                {items[hoverIdx].name.length > 20 ? items[hoverIdx].name.slice(0, 19) + '…' : items[hoverIdx].name}
              </text>
              <text x={8} y={29} fontSize={11} fill="#6b7280">
                {items[hoverIdx].appearances} episode{items[hoverIdx].appearances !== 1 ? 's' : ''} · rank #{hoverIdx + 1}
              </text>
            </g>
          </>
        )}
      </svg>
    </div>
  );
}
