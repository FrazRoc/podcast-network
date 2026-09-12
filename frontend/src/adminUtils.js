// A plain DATE like "2026-09-10" has no time component, so new Date(...)
// parses it as UTC midnight — toLocaleDateString() then shows the wrong
// day for anyone west of UTC. Parse the parts directly as local time instead.
export const formatDateOnly = (dateStr) => {
  if (!dateStr) return null;
  const [year, month, day] = dateStr.split('-').map(Number);
  return new Date(year, month - 1, day).toLocaleDateString();
};

// Wraps every occurrence of each highlight's name in text with a <mark> of
// the given className. highlights: [{name, className}]
export const highlightNames = (text, highlights) => {
  if (!text || !highlights?.length) return text;

  const patterns = highlights.map(h => ({
    re: new RegExp(h.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi'),
    className: h.className,
  }));

  const matches = [];
  patterns.forEach(({ re, className }) => {
    let m;
    while ((m = re.exec(text)) !== null) {
      matches.push({ start: m.index, end: m.index + m[0].length, text: m[0], className });
    }
  });

  if (!matches.length) return text;

  matches.sort((a, b) => a.start - b.start);

  const parts = [];
  let pos = 0;
  matches.forEach((m, i) => {
    if (m.start > pos) parts.push(text.slice(pos, m.start));
    parts.push(<mark key={i} className={m.className}>{m.text}</mark>);
    pos = m.end;
  });
  if (pos < text.length) parts.push(text.slice(pos));
  return parts;
};
