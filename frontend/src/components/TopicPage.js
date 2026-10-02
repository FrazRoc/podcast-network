import ProfileLayout, { Section, Stat, MiniBars, ShowMore, ShowThumb } from './ProfileLayout';
import OrgLogo from './OrgLogo';
import AdminEditLink from './AdminEditLink';
import { TopicChip, Swatch, categoryColor } from './Topics';
import { useProfile, personHref, orgHref, showHref, avatarUrl, fmtDate, fmtMonthYear, plural, imageUrl, imageFallback } from '../profileUtils';

const describe = (t) => [t.name,
  `Who talks about ${t.name} on clean-energy podcasts: ${plural(t.totals.guests, 'guest')} across `
  + `${plural(t.totals.episodes, 'episode')} on ${plural(t.totals.shows, 'show')}, with the organisations they come from.`];

function People({ title, people, aside }) {
  if (!people?.length) return null;
  return (
    <Section title={title} aside={aside}>
      <ul className="divide-y divide-gray-100">
        <ShowMore items={people} initial={10} render={(p) => (
          <li key={p.host_id} className="py-2 flex items-center gap-3">
            <img src={p.profile_image_url ? imageUrl(p.profile_image_url, 80) : avatarUrl(p.name)} alt=""
              onError={imageFallback(p.profile_image_url, p.name)}
              className="w-9 h-9 rounded-full object-cover bg-gray-100 flex-shrink-0" />
            <div className="flex-1 min-w-0">
              <a href={personHref(p.host_id, p.slug)} className="text-sm font-medium text-gray-900 hover:text-teal-700 hover:underline">{p.name}</a>
              {(p.title || p.company) && (
                <p className="text-xs text-gray-500 truncate">{[p.title, p.company].filter(Boolean).join(', ')}</p>
              )}
            </div>
            <span className="text-xs text-gray-400 flex-shrink-0">
              {plural(p.episodes, 'episode')}
              {p.as_main_topic > 0 && <span className="hidden sm:inline"> · main topic on {p.as_main_topic}</span>}
            </span>
          </li>
        )} />
      </ul>
    </Section>
  );
}

export default function TopicPage() {
  const state = useProfile('topics', '/topics/', describe, (id) => `/api/topics/${id}`);
  return (
    <ProfileLayout state={state} kindLabel="Topic">
      {(t) => (
        <>
          <section className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
            <p className="text-xs font-medium uppercase tracking-wide flex items-center gap-1.5" style={{ color: categoryColor(t.category) }}>
              <Swatch category={t.category} className="w-2.5 h-2.5" />
              <a href={`/topics?category=${encodeURIComponent(t.category)}`} className="hover:underline">{t.category}</a>
            </p>
            <h1 className="mt-1 text-2xl font-bold text-gray-900">{t.name}</h1>
            <AdminEditLink href={`/admin/topics?tag_id=${t.tag_id}`} />
            {t.aliases.length > 0 && (
              <p className="mt-1 text-sm text-gray-500">Also written as {t.aliases.join(', ')}</p>
            )}
            <p className="mt-3 text-[11px] text-gray-400">
              Read from episode descriptions; topics are still being added across the archive, so these counts will grow.
            </p>
          </section>

          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
            <Stat label="Episodes" value={t.totals.episodes} />
            <Stat label="Guests" value={t.totals.guests} />
            <Stat label="Shows" value={t.totals.shows} />
            <Stat label="Latest" value={fmtMonthYear(t.totals.last_date) || '—'} />
          </div>

          <People title="Who talks about it" people={t.guests} aside="guests" />

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-5 items-start">
            {t.shows.length > 0 && (
              <Section title="Shows that cover it" aside="episodes · share of their tagged">
                <ul className="space-y-2">
                  {t.shows.map(s => (
                    <li key={s.podcast_id} className="flex items-center gap-2 text-sm">
                      <ShowThumb show={s} size="w-7 h-7" />
                      <a href={showHref(s.podcast_id, s.slug)} className="flex-1 truncate text-gray-900 hover:text-teal-700 hover:underline">{s.title}</a>
                      <span className="text-xs text-gray-400 flex-shrink-0">{s.episodes}{s.share != null && ` · ${s.share}%`}</span>
                    </li>
                  ))}
                </ul>
              </Section>
            )}
            {t.orgs.length > 0 && (
              <Section title="Where its guests work" aside="distinct guests">
                <ul className="space-y-2">
                  {t.orgs.map(o => (
                    <li key={o.org_id} className="flex items-center gap-2 text-sm">
                      <OrgLogo orgId={o.org_id} name={o.name} size={24} />
                      <a href={orgHref(o.org_id, o.slug)} className="flex-1 truncate text-gray-900 hover:text-teal-700 hover:underline">{o.name}</a>
                      <span className="text-xs text-gray-400 flex-shrink-0">{o.people}</span>
                    </li>
                  ))}
                </ul>
              </Section>
            )}
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-5 items-start">
            {t.by_year.length > 0 && (
              <Section title="Episodes per year">
                <MiniBars rows={t.by_year} labelKey="year" valueKey="episodes" color={categoryColor(t.category)} />
              </Section>
            )}
            {t.related.length > 0 && (
              <Section title="Often discussed with" aside="episodes in common">
                <div className="flex flex-wrap gap-1.5">
                  {t.related.map(r => <TopicChip key={r.tag_id} topic={r} count={r.episodes} />)}
                </div>
              </Section>
            )}
          </div>

          <People title="Hosts who cover it" people={t.hosts} />

          {t.recent_episodes.length > 0 && (
            <Section title="Recent episodes" aside={t.totals.episodes > 20 ? `latest 20 of ${t.totals.episodes}` : undefined}>
              <ul className="divide-y divide-gray-100">
                {t.recent_episodes.map(e => (
                  <li key={e.episode_id} className="py-2.5 flex items-start justify-between gap-3">
                    <ShowThumb show={e.show} />
                    <div className="min-w-0 flex-1">
                      <p className="text-sm font-medium text-gray-900">
                        {e.title}
                        {e.main_topic && <span className="ml-2 text-[11px] px-1.5 py-0.5 rounded bg-teal-100 text-teal-800 align-middle">main topic</span>}
                      </p>
                      <p className="text-xs text-gray-500 mt-0.5">
                        <a href={showHref(e.show.podcast_id, e.show.slug)} className="text-teal-700 hover:underline">{e.show.title}</a>
                        {' · '}{fmtDate(e.published_date)}
                        {e.guests.length > 0 && ' · with '}
                        {e.guests.map((g, i) => (
                          <span key={g.host_id}>{i ? ', ' : ''}<a href={personHref(g.host_id, g.slug)} className="hover:underline">{g.name}</a></span>
                        ))}
                      </p>
                    </div>
                    <AdminEditLink href={`/admin/episodes?episode_id=${e.episode_id}`} className="flex-shrink-0">edit</AdminEditLink>
                    {e.listen_url && <a href={e.listen_url} target="_blank" rel="noopener noreferrer" className="flex-shrink-0 text-xs text-teal-700 hover:underline">Listen ↗</a>}
                  </li>
                ))}
              </ul>
            </Section>
          )}

          <p className="text-center text-sm">
            <a href={`/people?topic=${t.tag_id}`} className="text-teal-700 hover:underline">Everyone on these episodes</a>
            <span className="text-gray-300"> · </span>
            <a href="/topics" className="text-teal-700 hover:underline">All topics</a>
          </p>
        </>
      )}
    </ProfileLayout>
  );
}
