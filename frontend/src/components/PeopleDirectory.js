import { useEffect } from 'react';
import { SiteShell } from './SiteHeader';
import { useUrlParams, useDirectory, SearchBox, SortSelect, FilterChips, DirectoryHeading, DirectoryList } from './Directory';
import { ROLE_COLORS } from '../chartUtils';
import { personHref, orgHref, avatarUrl, fmtMonthYear, plural, setMeta, imageUrl, imageFallback, useShowOrgs } from '../profileUtils';

const SORTS = [['appearances', 'Most episodes'], ['recent', 'Most recent'], ['name', 'A–Z']];
const COLORS = { ...ROLE_COLORS, host: '#0d9488' };

export default function PeopleDirectory() {
  useShowOrgs();
  useEffect(() => setMeta('People · Podcast Network',
    'Everyone who has hosted or been a guest on a clean-energy podcast, with their current role and appearances.'), []);
  const [params, set] = useUrlParams({ q: '', kind: '', sort: 'appearances' });
  const state = useDirectory('people', params);

  const person = (p) => {
    const title = p.title || (p.kind === 'host' ? 'Host' : null);
    return (
      <li key={p.host_id} className="py-2.5 flex items-center gap-3">
        <img src={p.profile_image_url ? imageUrl(p.profile_image_url, 80) : avatarUrl(p.name)} alt=""
          onError={imageFallback(p.profile_image_url, p.name)}
          className="w-10 h-10 rounded-full object-cover bg-gray-100 flex-shrink-0" />
        <div className="flex-1 min-w-0">
          <a href={personHref(p.host_id, p.slug)} className="text-sm font-medium text-gray-900 hover:text-teal-700 hover:underline">{p.name}</a>
          {(title || p.company) && (
            <p className="text-xs text-gray-500 truncate">
              {title}{title && p.company ? ', ' : ''}
              {p.company && (p.org_id
                ? <a href={orgHref(p.org_id)} className="hover:text-teal-700 hover:underline">{p.company}</a>
                : p.company)}
            </p>
          )}
        </div>
        <div className="text-right text-xs flex-shrink-0">
          <p className="text-gray-600">{plural(p.appearances, 'episode')}<span className="hidden sm:inline"> · {plural(p.shows, 'show')}</span></p>
          {p.last_date && <p className="text-gray-400">{fmtMonthYear(p.last_date)}</p>}
        </div>
      </li>
    );
  };

  return (
    <SiteShell>
      <DirectoryHeading title="People"
        blurb="Hosts and guests across every show we track, with the role they were last introduced with." />
      <div className="flex gap-2">
        <SearchBox value={params.q} onChange={q => set({ q })} placeholder="Filter by name or company…" />
        <SortSelect value={params.sort} options={SORTS} onChange={sort => set({ sort })} />
      </div>
      <FilterChips chips={state.data?.kinds || []} value={params.kind} onChange={kind => set({ kind })} colors={COLORS} />
      <DirectoryList state={state} render={person} noun={state.total === 1 ? 'person' : 'people'} />
    </SiteShell>
  );
}
