import { useState, useEffect, useMemo } from 'react';
import { API_BASE_URL } from '../config';
import { ORG_TYPE_COLORS, ORG_TYPE_LABELS } from '../chartUtils';

const W = 640;
const H = 400;
const NODE_W = 12;
const GAP = 10;
const PAD = { top: 26, bottom: 8, left: 132, right: 132 };

// Career moves between kinds of organisation: each band is guests whose
// former role was at the left-hand type and whose current role is at the
// right-hand one. Click a band to see who.
export default function RevolvingDoorChart() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [hover, setHover] = useState(null);       // flow key
  const [picked, setPicked] = useState(null);     // flow key

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/revolving-door`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(d => { setData(d); if (d.flows.length) setPicked(`${d.flows[0].from}>${d.flows[0].to}`); })
      .catch(e => setError(e.message));
  }, []);

  const layout = useMemo(() => {
    if (!data || !data.flows.length) return null;
    const sum = (side) => {
      const t = {};
      data.flows.forEach(f => { t[f[side]] = (t[f[side]] || 0) + f.count; });
      return Object.entries(t).sort((a, b) => b[1] - a[1]);
    };
    const left = sum('from');
    const right = sum('to');
    const total = data.people;
    const avail = H - PAD.top - PAD.bottom;
    const scale = (avail - GAP * (Math.max(left.length, right.length) - 1)) / total;
    const place = (list, x) => {
      let y = PAD.top;
      const nodes = {};
      list.forEach(([type, n]) => {
        nodes[type] = { type, n, x, y, h: n * scale, inUsed: 0, outUsed: 0 };
        y += n * scale + GAP;
      });
      return nodes;
    };
    const L = place(left, PAD.left);
    const R = place(right, W - PAD.right - NODE_W);
    const order = (nodes) => (t) => nodes[t].y;
    const flows = [...data.flows]
      .sort((a, b) => order(L)(a.from) - order(L)(b.from) || order(R)(a.to) - order(R)(b.to))
      .map(f => {
        const s = L[f.from];
        const t = R[f.to];
        const h = f.count * scale;
        const y0 = s.y + s.outUsed;
        const y1 = t.y + t.inUsed;
        s.outUsed += h;
        t.inUsed += h;
        return { ...f, key: `${f.from}>${f.to}`, h, y0, y1 };
      });
    return { L, R, flows };
  }, [data]);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading…</p>;
  if (!layout) return <p className="text-sm text-gray-400">No career moves found yet.</p>;

  const x0 = PAD.left + NODE_W;
  const x1 = W - PAD.right - NODE_W;
  const mid = (x0 + x1) / 2;
  const band = f => `M${x0} ${f.y0} C${mid} ${f.y0} ${mid} ${f.y1} ${x1} ${f.y1}
    L${x1} ${f.y1 + f.h} C${mid} ${f.y1 + f.h} ${mid} ${f.y0 + f.h} ${x0} ${f.y0 + f.h} Z`;
  const active = hover || picked;
  const chosen = layout.flows.find(f => f.key === picked);

  return (
    <div>
      <div className="overflow-x-auto">
        <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W }} onMouseLeave={() => setHover(null)}>
          {layout.flows.map(f => (
            <path key={f.key} d={band(f)} fill={ORG_TYPE_COLORS[f.from]}
              opacity={active ? (f.key === active ? 0.75 : 0.22) : 0.4}
              style={{ cursor: 'pointer' }}
              onMouseEnter={() => setHover(f.key)} onClick={() => setPicked(f.key)}>
              <title>{`${ORG_TYPE_LABELS[f.from]} → ${ORG_TYPE_LABELS[f.to]}: ${f.count} guests`}</title>
            </path>
          ))}
          {[...Object.values(layout.L).map(n => ({ ...n, side: 'L' })), ...Object.values(layout.R).map(n => ({ ...n, side: 'R' }))]
            .map(n => (
              <g key={`${n.side}${n.type}`}>
                <rect x={n.x} y={n.y} width={NODE_W} height={Math.max(n.h, 1)} rx={2} fill={ORG_TYPE_COLORS[n.type]} />
                <text x={n.side === 'L' ? n.x - 6 : n.x + NODE_W + 6} y={n.y + n.h / 2 + 4} fontSize={11}
                  textAnchor={n.side === 'L' ? 'end' : 'start'} fill="#374151">
                  {ORG_TYPE_LABELS[n.type]} <tspan fill="#9ca3af">{n.n}</tspan>
                </text>
              </g>
            ))}
          <text x={PAD.left + NODE_W / 2} y={12} fontSize={10} fontWeight={600} fill="#6b7280" textAnchor="middle">WAS AT</text>
          <text x={W - PAD.right - NODE_W / 2} y={12} fontSize={10} fontWeight={600} fill="#6b7280" textAnchor="middle">NOW AT</text>
        </svg>
      </div>

      {chosen && (
        <div className="mt-4 border-t border-gray-100 pt-3">
          <p className="text-sm font-medium text-gray-900 mb-2">
            {ORG_TYPE_LABELS[chosen.from]} → {ORG_TYPE_LABELS[chosen.to]}
            <span className="text-gray-400 font-normal"> · {chosen.count} guest{chosen.count !== 1 ? 's' : ''}
              {chosen.people.length < chosen.count ? `, first ${chosen.people.length} shown` : ''}</span>
          </p>
          <ul className="grid sm:grid-cols-2 gap-x-6 gap-y-1">
            {chosen.people.map(p => (
              <li key={p.host_id} className="text-xs text-gray-600 truncate"
                title={`${p.name}: ${p.from_org} → ${p.to_org}`}>
                <span className="text-gray-900">{p.name}</span> · {p.from_org} → {p.to_org}
              </li>
            ))}
          </ul>
        </div>
      )}
      <p className="text-xs text-gray-400 mt-3">
        {data.people} guests whose episode text names a former role, and whose current role is at a
        different kind of organisation. Click a band to see who moved.
      </p>
    </div>
  );
}
