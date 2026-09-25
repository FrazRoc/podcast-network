import { useState, useMemo } from 'react';

// One 100% bar per row, split into categories. Click a legend entry to sort
// rows by that category's share; click it again to go back to the given
// order. Hovering a segment shows its numbers in the caption line.
export default function StackedShareBars({ rows, keys, colors, labels, initialLimit = 25, noun = 'guests' }) {
  const [sortKey, setSortKey] = useState(null);
  const [hover, setHover] = useState(null);     // { row, key }
  const [showAll, setShowAll] = useState(false);

  const sorted = useMemo(() => {
    if (!sortKey) return rows;
    const share = r => (r.counts[sortKey] || 0) / (r.total || 1);
    return [...rows].sort((a, b) => share(b) - share(a));
  }, [rows, sortKey]);
  const shown = showAll ? sorted : sorted.slice(0, initialLimit);
  const pct = (n, t) => Math.round((100 * n) / (t || 1));

  return (
    <div>
      <div className="flex flex-wrap gap-x-3 gap-y-1.5 mb-3">
        {keys.map(k => (
          <button key={k} onClick={() => setSortKey(s => (s === k ? null : k))}
            className={`flex items-center gap-1.5 text-xs rounded px-1.5 py-0.5 ${sortKey === k ? 'bg-gray-900 text-white' : 'text-gray-600 hover:bg-gray-100'}`}
            title={sortKey === k ? 'Back to the default order' : `Sort by share of ${labels[k]}`}>
            <span className="w-2.5 h-2.5 rounded-sm flex-shrink-0" style={{ background: colors[k] }} />
            {labels[k]}
          </button>
        ))}
      </div>
      <p className="text-xs text-gray-500 h-4 mb-2">
        {hover
          ? `${hover.row.label}: ${pct(hover.row.counts[hover.key] || 0, hover.row.total)}% ${labels[hover.key].toLowerCase()} `
            + `(${hover.row.counts[hover.key] || 0} of ${hover.row.total} ${noun})`
          : sortKey ? `Sorted by share of ${labels[sortKey].toLowerCase()}.` : 'Click a category to sort by it.'}
      </p>
      <div className="space-y-1" onMouseLeave={() => setHover(null)}>
        {shown.map(r => (
          <div key={r.id} className="flex items-center gap-2">
            <span className="w-40 sm:w-52 flex-shrink-0 text-xs text-gray-700 truncate text-right" title={r.label}>{r.label}</span>
            <div className="flex-1 flex h-4 rounded overflow-hidden bg-gray-100">
              {keys.map(k => (r.counts[k] ? (
                <div key={k} style={{ width: `${(100 * r.counts[k]) / r.total}%`, background: colors[k] }}
                  className={`h-full ${hover && (hover.row.id !== r.id || hover.key !== k) ? 'opacity-60' : ''}`}
                  onMouseEnter={() => setHover({ row: r, key: k })} />
              ) : null))}
            </div>
            <span className="w-10 flex-shrink-0 text-[11px] text-gray-400 tabular-nums">{r.total}</span>
          </div>
        ))}
      </div>
      {sorted.length > initialLimit && (
        <button onClick={() => setShowAll(s => !s)} className="mt-3 text-xs text-teal-700 hover:underline">
          {showAll ? 'Show fewer' : `Show all ${sorted.length}`}
        </button>
      )}
    </div>
  );
}
