import ProfileLayout, { Section, Stat, ShowMore } from './ProfileLayout';
import OrgLogo from './OrgLogo';
import AdminEditLink from './AdminEditLink';
import { useProfile, personHref, orgHref, showHref, avatarUrl, proxied, fmtDate, fmtMonthYear, plural } from '../profileUtils';

const describe = (p) => {
  const role = p.current_role && [p.current_role.title, p.current_role.company].filter(Boolean).join(', ');
  return [p.name, `${p.name}${role ? ` (${role})` : ''}: ${plural(p.totals.appearances, 'podcast appearance')} `
    + `across ${plural(p.totals.shows, 'show')}, with every episode, career on the record, and who they appear with.`];
};

const DateRange = ({ from, to }) => {
  const a = fmtMonthYear(from), b = fmtMonthYear(to);
  return <span>{a === b ? a : `${a} – ${b}`}</span>;
};

export default function PersonPage() {
  const state = useProfile('people', '/people/', describe);
  return (
    <ProfileLayout state={state} kindLabel="Person">
      {(p) => (
        <>
          {/* Header */}
          <section className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6 flex flex-col sm:flex-row gap-4 sm:gap-6 items-center sm:items-start">
            <img
              src={p.profile_image_url ? proxied(p.profile_image_url) : avatarUrl(p.name)} alt={p.name}
              onError={e => { e.target.onerror = null; e.target.src = avatarUrl(p.name); }}
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
              {p.described_as?.length > 0 && (
                <p className="mt-1 text-sm text-gray-500">{p.described_as.join(' · ')}</p>
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

          {p.career.length > 0 && (
            <Section title="Career on the record" aside="as introduced on episodes">
              <ul className="divide-y divide-gray-100">
                {p.career.map((c, i) => (
                  <li key={i} className="py-2 flex items-start gap-3">
                    <OrgLogo orgId={c.org_id} name={c.company || c.title} size={28} className="mt-0.5" />
                    <div className="flex-1 min-w-0">
                      <p className="text-sm text-gray-900">
                        {c.title && <span className="font-medium">{c.title}</span>}
                        {c.title && c.company && <span className="text-gray-400"> at </span>}
                        {c.company && (c.org_id
                          ? <a href={orgHref(c.org_id)} className="text-teal-700 hover:underline">{c.company}</a>
                          : <span>{c.company}</span>)}
                        {c.current && <span className="ml-2 text-[11px] px-1.5 py-0.5 rounded bg-teal-100 text-teal-800 align-middle">current</span>}
                        {c.former && <span className="ml-2 text-[11px] px-1.5 py-0.5 rounded bg-gray-100 text-gray-600 align-middle">former</span>}
                      </p>
                      <p className="text-xs text-gray-500">
                        <DateRange from={c.first_date} to={c.last_date} /> · {plural(c.mentions, 'mention')}
                      </p>
                    </div>
                  </li>
                ))}
              </ul>
            </Section>
          )}

          <Section title="Appearances" aside={plural(p.appearances.length, 'episode')}>
            <ul className="divide-y divide-gray-100">
              <ShowMore items={p.appearances} initial={15} render={(a) => (
                <li key={a.episode_id} className="py-2.5">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <p className="text-sm font-medium text-gray-900">{a.title}</p>
                      <p className="text-xs text-gray-500 mt-0.5">
                        <a href={showHref(a.show.podcast_id, a.show.slug)} className="text-teal-700 hover:underline">{a.show.title}</a>
                        {' · '}{fmtDate(a.published_date)}{' · '}{a.role}
                        {a.as && (a.as.title || a.as.company) && (
                          <> · as {[a.as.title, a.as.company].filter(Boolean).join(', ')}</>
                        )}
                      </p>
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

          <div className="grid sm:grid-cols-2 gap-5">
            {p.appears_with.length > 0 && (
              <Section title="Often appears with">
                <ul className="space-y-2">
                  {p.appears_with.map(c => (
                    <li key={c.host_id} className="flex items-center gap-2 text-sm">
                      <img src={c.profile_image_url ? proxied(c.profile_image_url) : avatarUrl(c.name)} alt=""
                        onError={e => { e.target.onerror = null; e.target.src = avatarUrl(c.name); }}
                        className="w-7 h-7 rounded-full object-cover bg-gray-100" />
                      <a href={personHref(c.host_id, c.slug)} className="flex-1 truncate text-gray-900 hover:text-teal-700 hover:underline">{c.name}</a>
                      <span className="text-xs text-gray-400">{plural(c.episodes, 'episode')}{c.always_host ? ' · host' : ''}</span>
                    </li>
                  ))}
                </ul>
              </Section>
            )}
            <Section title="Shows">
              <ul className="space-y-2">
                {p.shows.map(s => (
                  <li key={s.podcast_id} className="text-sm">
                    <a href={showHref(s.podcast_id, s.slug)} className="text-gray-900 hover:text-teal-700 hover:underline">{s.title}</a>
                    <p className="text-xs text-gray-500">
                      {plural(s.appearances, 'episode')}{s.as_host ? ` (host on ${s.as_host})` : ''} · <DateRange from={s.first_date} to={s.last_date} />
                    </p>
                  </li>
                ))}
              </ul>
            </Section>
          </div>
        </>
      )}
    </ProfileLayout>
  );
}
