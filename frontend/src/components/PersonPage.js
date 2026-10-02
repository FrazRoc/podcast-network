import { useMemo, useState } from 'react';
import ProfileLayout, { Section, Stat, ShowMore, ShowThumb } from './ProfileLayout';
import OrgLogo from './OrgLogo';
import AdminEditLink from './AdminEditLink';
import { TalksAbout, EpisodeTopics, DiscussedIn } from './Topics';
import { useProfile, personHref, orgHref, showHref, avatarUrl, fmtDate, fmtMonthYear, plural, imageUrl, imageFallback } from '../profileUtils';

const describe = (p) => {
  const role = p.current_role && [p.current_role.title, p.current_role.company].filter(Boolean).join(', ');
  return [p.name, `${p.name}${role ? ` (${role})` : ''}: ${plural(p.totals.appearances, 'podcast appearance')} `
    + `across ${plural(p.totals.shows, 'show')}, with every episode, career on the record, who interviewed them and who they appear with.`];
};

const DateRange = ({ from, to }) => {
  const a = fmtMonthYear(from), b = fmtMonthYear(to);
  return <span>{a === b ? a : `${a} – ${b}`}</span>;
};

const Badge = ({ kind }) => (kind === 'current'
  ? <span className="ml-2 text-[11px] px-1.5 py-0.5 rounded bg-teal-100 text-teal-800 align-middle">current</span>
  : <span className="ml-2 text-[11px] px-1.5 py-0.5 rounded bg-gray-100 text-gray-600 align-middle">former</span>);

// One entry per organisation, with the positions they were introduced with
// there (or, for someone with no organisation on record, one per title).
function Career({ items }) {
  // Titles alone ("Consulting", "Expert") add nothing to the line under the
  // name, so the card is for people with an organisation on record.
  if (!items.some(c => c.company)) return null;
  return (
    <Section title="Career on the record" aside="as introduced on episodes">
      <ul className="divide-y divide-gray-100">
        {items.map((c, i) => (
          <li key={c.org_id || c.company || c.title || i} className="py-2.5 flex items-start gap-3">
            <OrgLogo orgId={c.org_id} name={c.company || c.title} size={28} className="mt-0.5" />
            <div className="flex-1 min-w-0">
              <p className="text-sm text-gray-900">
                {c.company
                  ? (c.org_id
                    ? <a href={orgHref(c.org_id)} className="font-medium text-teal-700 hover:underline">{c.company}</a>
                    : <span className="font-medium">{c.company}</span>)
                  : <span className="font-medium">{c.title}</span>}
                {c.current && <Badge kind="current" />}
                {c.former && <Badge kind="former" />}
              </p>
              {c.titles.length > 0 && (
                <p className="text-sm text-gray-700">
                  {c.titles.map((t, j) => (
                    <span key={t.title}>
                      {j > 0 && <span className="text-gray-300"> · </span>}
                      <span className={t.current ? 'font-medium text-gray-900' : ''}
                        title={`${fmtMonthYear(t.first_date)}${t.last_date && t.last_date !== t.first_date ? ` – ${fmtMonthYear(t.last_date)}` : ''}`}>
                        {t.title}
                      </span>
                    </span>
                  ))}
                </p>
              )}
              <p className="text-xs text-gray-500"><DateRange from={c.first_date} to={c.last_date} /></p>
            </div>
          </li>
        ))}
      </ul>
    </Section>
  );
}

function PeopleList({ title, people }) {
  if (!people?.length) return null;
  return (
    <Section title={title}>
      <ul className="space-y-2">
        {people.map(c => (
          <li key={c.host_id} className="flex items-center gap-2 text-sm">
            <img src={c.profile_image_url ? imageUrl(c.profile_image_url, 64) : avatarUrl(c.name)} alt=""
              onError={imageFallback(c.profile_image_url, c.name)}
              className="w-7 h-7 rounded-full object-cover bg-gray-100" />
            <a href={personHref(c.host_id, c.slug)} className="flex-1 truncate text-gray-900 hover:text-teal-700 hover:underline">{c.name}</a>
            <span className="text-xs text-gray-400">{plural(c.episodes, 'episode')}</span>
          </li>
        ))}
      </ul>
    </Section>
  );
}

const TABS = [['Guest', 'As guest'], ['Host', 'As host']];

// Career, then appearances split into guest and host (when they've been
// both), and the people lists for whichever side is showing.
function PersonDetails({ p }) {
  const counts = { Guest: p.totals.as_guest, Host: p.totals.appearances - p.totals.as_guest };
  const both = counts.Guest > 0 && counts.Host > 0;
  const [tab, setTab] = useState(counts.Guest > 0 ? 'Guest' : 'Host');
  const [show, setShow] = useState('');

  const inTab = useMemo(() => p.appearances.filter(a => !both || a.role === tab), [p.appearances, both, tab]);
  const tabShows = useMemo(() => {
    const byId = {};
    inTab.forEach(a => { (byId[a.show.podcast_id] ||= { ...a.show, n: 0 }).n += 1; });
    return Object.values(byId).sort((a, b) => b.n - a.n || a.title.localeCompare(b.title));
  }, [inTab]);
  const listed = show ? inTab.filter(a => String(a.show.podcast_id) === show) : inTab;
  const pickTab = (t) => { setTab(t); setShow(''); };

  const lists = tab === 'Guest'
    ? [['Interviewed by', p.interviewed_by], ['Appeared alongside', p.appeared_alongside]]
    : [['Co-hosts', p.co_hosts], ['Their most frequent guests', p.guests_hosted]];

  const wideShows = lists.filter(([, people]) => people?.length).length % 2 === 0;

  return (
    <>
      <Career items={p.career_by_org || []} />

      <TalksAbout summary={p.talks_about} />
      <DiscussedIn discussed={p.discussed_in} name={p.name} />

      <Section title="Appearances" aside={plural(listed.length, 'episode')}>
        {(both || tabShows.length > 1) && (
          <div className="flex flex-wrap items-center gap-2 mb-2">
            {both && (
              <div className="flex rounded-lg bg-gray-100 p-0.5 text-sm">
                {TABS.map(([t, label]) => (
                  <button key={t} onClick={() => pickTab(t)}
                    className={`rounded-md px-3 py-1 font-medium transition-colors ${
                      tab === t ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}>
                    {label} <span className="text-gray-400 font-normal">{counts[t].toLocaleString()}</span>
                  </button>
                ))}
              </div>
            )}
            {tabShows.length > 1 && (
              <select value={show} onChange={e => setShow(e.target.value)} aria-label="Show"
                className="min-w-0 max-w-full rounded-lg border border-gray-300 bg-white px-2 py-1 text-sm text-gray-700 focus:border-teal-500 focus:outline-none">
                <option value="">All shows ({tabShows.length})</option>
                {tabShows.map(s => <option key={s.podcast_id} value={s.podcast_id}>{s.title} ({s.n})</option>)}
              </select>
            )}
          </div>
        )}
        <ul className="divide-y divide-gray-100">
          <ShowMore key={`${tab}-${show}`} items={listed} initial={15} render={(a) => (
            <li key={a.episode_id} className="py-2.5">
              <div className="flex items-start justify-between gap-3">
                <ShowThumb show={a.show} />
                <div className="min-w-0 flex-1">
                  <p className="text-sm font-medium text-gray-900">{a.title}</p>
                  <p className="text-xs text-gray-500 mt-0.5">
                    <a href={showHref(a.show.podcast_id, a.show.slug)} className="text-teal-700 hover:underline">{a.show.title}</a>
                    {' · '}{fmtDate(a.published_date)}
                    {a.as && (a.as.title || a.as.company) && (
                      <> · as {[a.as.title, a.as.company].filter(Boolean).join(', ')}</>
                    )}
                  </p>
                  <EpisodeTopics topics={a.topics} companies={a.companies} people={a.people_mentioned} />
                </div>
                <AdminEditLink href={`/admin/episodes?episode_id=${a.episode_id}`} className="flex-shrink-0">edit</AdminEditLink>
                {a.listen_url && (
                  <a href={a.listen_url} target="_blank" rel="noopener noreferrer"
                     className="flex-shrink-0 text-xs text-teal-700 hover:underline">Listen ↗</a>
                )}
              </div>
            </li>
          )} />
        </ul>
      </Section>

      <div className="grid sm:grid-cols-2 gap-5 items-start">
        {lists.map(([title, people]) => <PeopleList key={title} title={title} people={people} />)}
        {/* Full width (two columns inside) when it would otherwise sit alone in a row. */}
        <div className={wideShows ? 'sm:col-span-2' : ''}>
        <Section title="Shows">
          <ul className={`grid gap-2 ${wideShows ? 'sm:grid-cols-2 sm:gap-x-6' : ''}`}>
            {p.shows.map(s => (
              <li key={s.podcast_id} className="text-sm flex items-center gap-3">
                <ShowThumb show={s} />
                <div className="min-w-0">
                  <a href={showHref(s.podcast_id, s.slug)} className="text-gray-900 hover:text-teal-700 hover:underline">{s.title}</a>
                  <p className="text-xs text-gray-500">
                    {plural(s.appearances, 'episode')}{s.as_host ? ` (host on ${s.as_host})` : ''} · <DateRange from={s.first_date} to={s.last_date} />
                  </p>
                </div>
              </li>
            ))}
          </ul>
        </Section>
        </div>
      </div>
    </>
  );
}

export default function PersonPage() {
  const state = useProfile('people', '/people/', describe);
  return (
    <ProfileLayout state={state} kindLabel="Person">
      {(p) => {
        // The header role already says it; don't repeat it underneath.
        const roleKey = (p.current_role?.title || '').toLowerCase();
        const described = (p.described_as || []).filter(d => d.toLowerCase() !== roleKey);
        return (
        <>
          {/* Header */}
          <section className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6 flex flex-col sm:flex-row gap-4 sm:gap-6 items-center sm:items-start">
            <img
              src={p.profile_image_url ? imageUrl(p.profile_image_url, 240) : avatarUrl(p.name)} alt={p.name}
              onError={imageFallback(p.profile_image_url, p.name)}
              className="w-28 h-28 rounded-full object-cover bg-gray-100 flex-shrink-0"
            />
            <div className="flex-1 min-w-0 text-center sm:text-left">
              <h1 className="text-2xl font-bold text-gray-900">{p.name}</h1>
              <AdminEditLink href={`/admin/people?host_id=${p.host_id}`} />
              {p.current_role && (p.current_role.title || p.current_role.company) && (
                <p className="mt-1 text-gray-700 flex items-center justify-center sm:justify-start gap-1.5 flex-wrap">
                  {p.current_role.org_id && <OrgLogo orgId={p.current_role.org_id} name={p.current_role.company} size={20} />}
                  <span>{p.current_role.title}</span>
                  {p.current_role.title && p.current_role.company && <span className="text-gray-400">·</span>}
                  {p.current_role.company && (p.current_role.org_id
                    ? <a href={orgHref(p.current_role.org_id)} className="text-teal-700 hover:underline">{p.current_role.company}</a>
                    : <span>{p.current_role.company}</span>)}
                </p>
              )}
              {described.length > 0 && (
                <p className="mt-1 text-sm text-gray-500">{described.join(' · ')}</p>
              )}
              {p.bio && <p className="mt-3 text-sm text-gray-600 leading-relaxed">{p.bio}</p>}
              <div className="mt-3 flex flex-wrap justify-center sm:justify-start gap-3 text-sm">
                {p.linkedin_url && <a href={p.linkedin_url} target="_blank" rel="noopener noreferrer" className="text-teal-700 hover:underline">LinkedIn</a>}
                {p.website_url && <a href={p.website_url} target="_blank" rel="noopener noreferrer" className="text-teal-700 hover:underline">Website</a>}
                {p.twitter_handle && <a href={`https://x.com/${p.twitter_handle}`} target="_blank" rel="noopener noreferrer" className="text-teal-700 hover:underline">X</a>}
                {p.bluesky_handle && <a href={`https://bsky.app/profile/${p.bluesky_handle}`} target="_blank" rel="noopener noreferrer" className="text-teal-700 hover:underline">Bluesky</a>}
              </div>
            </div>
          </section>

          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
            <Stat label="Episodes" value={p.totals.appearances} />
            <Stat label="Shows" value={p.totals.shows} />
            <Stat label="First" value={fmtMonthYear(p.totals.first_date) || '—'} />
            <Stat label="Latest" value={fmtMonthYear(p.totals.last_date) || '—'} />
          </div>

          <PersonDetails p={p} />
        </>
        );
      }}
    </ProfileLayout>
  );
}
