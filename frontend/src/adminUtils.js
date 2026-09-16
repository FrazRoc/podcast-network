// A plain DATE like "2026-09-10" has no time component, so new Date(...)
// parses it as UTC midnight — toLocaleDateString() then shows the wrong
// day for anyone west of UTC. Parse the parts directly as local time instead.
export const formatDateOnly = (dateStr) => {
  if (!dateStr) return null;
  const [year, month, day] = dateStr.split('-').map(Number);
  return new Date(year, month - 1, day).toLocaleDateString();
};

// Feeds like SunCast's ship raw HTML in the description (<p>, <ul><li>,
// <a href>...). The scanner already handles this fine for name-matching
// (backend/description_cleaner.py's strip_html), but the admin UI was
// showing the literal, unstripped markup to reviewers — real incident:
// suggestion 4603 on SunCast episode 92044 was unreadable, wall of <p> tags.
// This is display-only cleaning, separate from the matching logic: it keeps
// paragraph/list breaks as newlines (readable) rather than collapsing
// everything to spaces (which is what the matcher wants instead).
const _HTML_ENTITIES = {
  '&nbsp;': ' ', '&amp;': '&', '&quot;': '"', '&#39;': "'", '&rsquo;': "'",
  '&lsquo;': "'", '&mdash;': '—', '&ndash;': '–', '&hellip;': '…', '&lt;': '<', '&gt;': '>',
};

export const stripHtmlForDisplay = (html) => {
  if (!html) return html;
  let text = html
    .replace(/<\/(p|div|li|h[1-6])>/gi, '\n')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<li[^>]*>/gi, '• ')
    .replace(/<[^>]+>/g, '');
  for (const [entity, char] of Object.entries(_HTML_ENTITIES)) {
    text = text.split(entity).join(char);
  }
  return text
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
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

// suggestions.source is tagged "title_<pattern>" / "desc_<pattern>" (e.g.
// "title_dash", "desc_bio_sentence") — see extract_candidate_names_tagged()
// in episode_name_scanner.py. Older rows predate the tagging and are still
// just the bare "parsed_title"/"parsed_desc". This splits a value into
// where it was found and which specific heuristic found it there, for
// display in the suggestions list/review UI.
const PATTERN_LABELS = {
  labelled:     'Labelled credit',
  intro:        'Trigger phrase',
  joins:        '"joins us"',
  possessive:   "Org's Name",
  and:          '"...and Name"',
  guest_list:   'Guest list',
  bio_sentence: 'Bio sentence',
  dash:         'Title dash/comma',
};

export const formatSuggestionSource = (source) => {
  if (source === 'parsed_title') return { scope: 'Episode Title', pattern: null };
  if (source === 'parsed_desc') return { scope: 'Description', pattern: null };
  const [scopeKey, ...rest] = (source || '').split('_');
  const pattern = rest.join('_');
  const scope = scopeKey === 'title' ? 'Episode Title' : scopeKey === 'desc' ? 'Description' : source;
  return { scope, pattern: PATTERN_LABELS[pattern] || pattern || null };
};
