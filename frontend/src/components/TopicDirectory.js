import { useEffect } from 'react';
import { SiteShell } from './SiteHeader';
import { useUrlParams, useDirectory, SearchBox, SortSelect, FilterChips, DirectoryHeading, DirectoryList } from './Directory';
import { Swatch, CATEGORY_COLORS, CATEGORIES, categoryColor } from './Topics';
import { topicHref, fmtMonthYear, plural, setMeta } from '../profileUtils';

const SORTS = [['episodes', 'Most episodes'], ['people', 'Most people'], ['recent', 'Most recent'], ['name', 'A–Z']];

// The broad topics, under each category: the way in for browsing. Shown
// above the list unless the reader is searching; a category chip narrows
// it to that category.
function Browse({ broad, category }) {
  if (!broad?.length) return null;
  const groups = CATEGORIES.filter(c => !category || c === category)
    .map(c => [c, broad.filter(b => b.category === c)])
    .filter(([, items]) => items.length);
  if (!groups.length) return null;
  return (
    <section className="mt-4 bg-white rounded-2xl border border-gray-200 p-4 sm:p-5">
      <h2 className="text-sm font-semibold text-gray-900">Browse</h2>
      <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-4">
        {groups.map(([c, items]) => (
          <div key={c} className="min-w-0">
            <p className="text-xs font-medium uppercase tracking-wide flex items-center gap-1.5" style={{ color: categoryColor(c) }}>
              <Swatch category={c} className="w-2.5 h-2.5" />{c}
            </p>
            <ul className="mt-1.5 space-y-1">
              {items.map(b => (
                <li key={b.tag_id} className="flex items-baseline justify-between gap-2 text-sm">
                  <a href={topicHref(b.tag_id, b.slug)} className="truncate text-gray-900 hover:text-teal-700 hover:underline">{b.name}</a>
                  <span className="text-xs text-gray-400 flex-shrink-0">{plural(b.episodes, 'episode')}</span>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>
    </section>
  );
}

export default function TopicDirectory() {
  useEffect(() => setMeta('Topics · Podcast Network',
    'What clean-energy and climate podcasts talk about: topics from geothermal to permitting, with the people and shows behind each.'), []);
  const [params, set] = useUrlParams({ q: '', category: '', sort: 'episodes' });
  const state = useDirectory('/api/topics', params, 100);
  const chips = (state.data?.categories || []).map(c => ({ kind: c.category, label: c.category, count: c.count }));

  const topic = (t) => (
    <li key={t.tag_id} className="py-2.5 flex items-center gap-3">
      <Swatch category={t.category} className="w-2.5 h-2.5" />
      <div className="flex-1 min-w-0">
        <a href={topicHref(t.tag_id, t.slug)} className="text-sm font-medium text-gray-900 hover:text-teal-700 hover:underline">{t.name}</a>
        <p className="text-xs text-gray-500 truncate">
          {t.category}
          {t.is_broad && <span> · broad topic</span>}
          {t.children > 0 && <span> · {plural(t.children, 'topic')} under it</span>}
          {t.aliases?.length > 0 && <span className="text-gray-400"> · also {t.aliases.slice(0, 3).join(', ')}</span>}
        </p>
      </div>
      <div className="text-right text-xs flex-shrink-0">
        <p className="text-gray-600">{plural(t.episodes, 'episode')}<span className="hidden sm:inline"> · {plural(t.people, 'person', 'people')} · {plural(t.shows, 'show')}</span></p>
        {t.last_date && <p className="text-gray-400">{fmtMonthYear(t.last_date)}</p>}
      </div>
    </li>
  );

  return (
    <SiteShell>
      <DirectoryHeading title="Topics"
        blurb="What the episodes are about, read from their descriptions. Topics are grouped into broad topics under twelve categories, and each one's counts include the narrower topics under it; topics on a single episode aren't listed." />
      <div className="flex gap-2">
        <SearchBox value={params.q} onChange={q => set({ q })} placeholder="Filter topics…" />
        <SortSelect value={params.sort} options={SORTS} onChange={sort => set({ sort })} />
      </div>
      <FilterChips chips={chips} value={params.category} onChange={category => set({ category })} colors={CATEGORY_COLORS} />
      {!params.q && <Browse broad={state.data?.broad} category={params.category} />}
      <DirectoryList state={state} render={topic} noun={state.total === 1 ? 'topic' : 'topics'} />
    </SiteShell>
  );
}
