import { useEffect } from 'react';
import { SiteShell } from './SiteHeader';
import { useUrlParams, useDirectory, SearchBox, SortSelect, FilterChips, DirectoryHeading, DirectoryList } from './Directory';
import { Swatch, CATEGORY_COLORS } from './Topics';
import { topicHref, fmtMonthYear, plural, setMeta } from '../profileUtils';

const SORTS = [['episodes', 'Most episodes'], ['people', 'Most people'], ['recent', 'Most recent'], ['name', 'A–Z']];

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
        blurb="What the episodes are about, read from their descriptions. Each topic is filed under one of twelve categories; topics on a single episode aren't listed. Topics are still being added across the archive, so counts will grow." />
      <div className="flex gap-2">
        <SearchBox value={params.q} onChange={q => set({ q })} placeholder="Filter topics…" />
        <SortSelect value={params.sort} options={SORTS} onChange={sort => set({ sort })} />
      </div>
      <FilterChips chips={chips} value={params.category} onChange={category => set({ category })} colors={CATEGORY_COLORS} />
      <DirectoryList state={state} render={topic} noun={state.total === 1 ? 'topic' : 'topics'} />
    </SiteShell>
  );
}
