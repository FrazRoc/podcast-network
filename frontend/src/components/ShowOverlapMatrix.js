import { useState, useEffect, useMemo } from 'react';
import { API_BASE_URL } from '../config';

const CELL = 28;

// Sequential single-hue scale (teal) — light to dark by magnitude.
// Never a rainbow: identity isn't being encoded here, only magnitude.
const tealScale = (t) => {
  const lightness = 92 - t * 62; // 92% (near-white) -> 30% (deep teal)
  return `hsl(173, 65%, ${lightness}%)`;
};

export default function ShowOverlapMatrix() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [hovered, setHovered] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/show-overlap?top_n=25`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  const { shows, matrix, maxShared } = useMemo(() => {
    if (!data) return { shows: [], matrix: {}, maxShared: 1 };
    const m = {};
    let max = 1;
    data.pairs.forEach(p => {
      m[`${p.podcast_a}-${p.podcast_b}`] = p.shared;
      m[`${p.podcast_b}-${p.podcast_a}`] = p.shared;
      if (p.shared > max) max = p.shared;
    });
    return { shows: data.shows, matrix: m, maxShared: max };
  }, [data]);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading...</p>;

  return (
    <div>
      <div className="overflow-x-auto">
        <div style={{ display: 'inline-block', paddingTop: 90, paddingLeft: 4 }}>
          <div style={{ display: 'flex' }}>
            {/* Row label column spacer */}
            <div style={{ width: 160, flexShrink: 0 }} />
            {/* Column headers, rotated */}
            {shows.map(s => (
              <div key={s.podcast_id}
                style={{ width: CELL, flexShrink: 0, position: 'relative', height: 90 }}>
                <div
                  title={s.title}
                  style={{
                    position: 'absolute', bottom: 4, left: '50%',
                    transform: 'rotate(-45deg)', transformOrigin: 'left bottom',
                    whiteSpace: 'nowrap', fontSize: 11, color: '#6b7280',
                    maxWidth: 130, overflow: 'hidden', textOverflow: 'ellipsis',
                  }}
                >
                  {s.title}
                </div>
              </div>
            ))}
          </div>
          {shows.map(rowShow => (
            <div key={rowShow.podcast_id} style={{ display: 'flex' }}>
              <div
                title={rowShow.title}
                style={{
                  width: 160, flexShrink: 0, height: CELL, lineHeight: `${CELL}px`,
                  fontSize: 11, color: '#374151', overflow: 'hidden',
                  textOverflow: 'ellipsis', whiteSpace: 'nowrap', paddingRight: 8,
                  textAlign: 'right',
                }}
              >
                {rowShow.title}
              </div>
              {shows.map(colShow => {
                const isSelf = rowShow.podcast_id === colShow.podcast_id;
                const shared = isSelf ? null : (matrix[`${rowShow.podcast_id}-${colShow.podcast_id}`] || 0);
                const isHovered = hovered
                  && (hovered.a === rowShow.podcast_id || hovered.a === colShow.podcast_id)
                  && (hovered.b === rowShow.podcast_id || hovered.b === colShow.podcast_id);
                return (
                  <div
                    key={colShow.podcast_id}
                    onMouseEnter={() => !isSelf && setHovered({ a: rowShow.podcast_id, b: colShow.podcast_id, shared })}
                    onMouseLeave={() => setHovered(null)}
                    title={isSelf ? `${rowShow.title}: ${rowShow.people_count} people` : `${rowShow.title} × ${colShow.title}: ${shared} shared guest${shared !== 1 ? 's' : ''}`}
                    style={{
                      width: CELL, height: CELL, flexShrink: 0,
                      background: isSelf ? '#e5e7eb' : tealScale(shared / maxShared),
                      border: isHovered ? '2px solid #0d9488' : '1px solid white',
                      boxSizing: 'border-box',
                      cursor: isSelf ? 'default' : 'pointer',
                    }}
                  />
                );
              })}
            </div>
          ))}
        </div>
      </div>

      {/* Legend */}
      <div className="flex items-center gap-2 mt-4 text-xs text-gray-500">
        <span>Fewer shared guests</span>
        <div className="flex">
          {[0, 0.25, 0.5, 0.75, 1].map(t => (
            <div key={t} style={{ width: 20, height: 12, background: tealScale(t) }} />
          ))}
        </div>
        <span>More shared guests (max {maxShared})</span>
      </div>
    </div>
  );
}
