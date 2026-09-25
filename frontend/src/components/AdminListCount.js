import React from 'react';

// The count line directly above every admin list: how many rows match the
// current search and filters, out of how many in total (only when a filter
// actually narrows it), and whether only part of the list is loaded.
//
//   6,469 companies · showing first 300
//   1,234 of 6,469 companies match · showing first 300
export default function AdminListCount({ total, allTotal, shown, noun, plural, className = 'mb-2' }) {
  if (total == null) return null;
  const fmt = n => Number(n).toLocaleString();
  const word = n => (n === 1 ? noun : (plural || `${noun}s`));
  const narrowed = allTotal != null && allTotal !== total;
  return (
    <p className={`text-sm text-gray-500 ${className}`}>
      <span className="font-semibold text-gray-900">{fmt(total)}</span>
      {narrowed ? ` of ${fmt(allTotal)} ${word(allTotal)} match` : ` ${word(total)}`}
      {shown != null && shown < total && (
        <span className="text-gray-400"> · showing first {fmt(shown)}</span>
      )}
    </p>
  );
}
