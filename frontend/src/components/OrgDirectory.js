import { useEffect } from 'react';
import { SiteShell } from './SiteHeader';
import OrgLogo from './OrgLogo';
import { useUrlParams, useDirectory, SearchBox, SortSelect, FilterChips, DirectoryHeading, DirectoryList } from './Directory';
import { ORG_TYPE_LABELS, ORG_TYPE_COLORS } from '../chartUtils';
import { orgHref, fmtMonthYear, plural, setMeta } from '../profileUtils';

const SORTS = [['people', 'Most people'], ['recent', 'Most recent'], ['name', 'A–Z']];

export default function OrgDirectory() {
  useEffect(() => setMeta('Organisations · Podcast Network',
    'The companies, investors, universities and agencies whose people are guests on clean-energy podcasts.'), []);
  const [params, set] = useUrlParams({ q: '', type: '', sort: 'people' });
  const state = useDirectory('orgs', params);

  const org = (o) => (
    <li key={o.org_id} className="py-2.5 flex items-center gap-3">
      <OrgLogo orgId={o.org_id} name={o.name} size={40} />
      <div className="flex-1 min-w-0">
        <a href={orgHref(o.org_id, o.slug)} className="text-sm font-medium text-gray-900 hover:text-teal-700 hover:underline">{o.name}</a>
        <p className="text-xs text-gray-500 truncate flex items-center gap-1.5">
          {o.org_type && <span className="w-2 h-2 rounded-sm flex-shrink-0" style={{ background: ORG_TYPE_COLORS[o.org_type] || '#bab0ac' }} />}
          {[ORG_TYPE_LABELS[o.org_type] || o.org_type, o.parent_name && `part of ${o.parent_name}`].filter(Boolean).join(' · ')}
        </p>
      </div>
      <div className="text-right text-xs flex-shrink-0">
        <p className="text-gray-600">{plural(o.people, 'person', 'people')}<span className="hidden sm:inline"> · {plural(o.appearances, 'appearance')}</span></p>
        {o.last_date && <p className="text-gray-400">{fmtMonthYear(o.last_date)}</p>}
      </div>
    </li>
  );

  return (
    <SiteShell>
      <DirectoryHeading title="Organisations"
        blurb="Where guests work, by how many of their people have been on. Counts are guests only, so a show's own hosts don't inflate them." />
      <div className="flex gap-2">
        <SearchBox value={params.q} onChange={q => set({ q })} placeholder="Filter by name…" />
        <SortSelect value={params.sort} options={SORTS} onChange={sort => set({ sort })} />
      </div>
      <FilterChips chips={state.data?.types || []} value={params.type} onChange={type => set({ type })}
        labels={{ ...ORG_TYPE_LABELS, other: 'Other' }} colors={ORG_TYPE_COLORS} />
      <DirectoryList state={state} render={org} noun={state.total === 1 ? 'organisation' : 'organisations'} />
    </SiteShell>
  );
}
