import { useEffect } from 'react';
import ProfileLayout, { Section, Stat, MiniBars, ShowMore, ShowThumb } from './ProfileLayout';
import OrgLogo from './OrgLogo';
import AdminEditLink from './AdminEditLink';
import { TalksAbout, DiscussedIn } from './Topics';
import { ORG_TYPE_LABELS, ORG_TYPE_COLORS } from '../chartUtils';
import { useProfile, personHref, orgHref, showHref, avatarUrl, fmtDate, fmtMonthYear, plural, imageUrl, imageFallback } from '../profileUtils';

const describe = (o) => [o.name, o.totals.people === 0 && o.discussed_in?.episodes
  ? `${o.name} on clean-energy podcasts: discussed on ${plural(o.discussed_in.episodes, 'episode')}.`
  : `Who from ${o.name} has been on clean-energy podcasts: ${plural(o.totals.people, 'person', 'people')}, `
    + `${plural(o.totals.appearances, 'appearance')} across ${plural(o.totals.shows, 'show')}.`];

export default function OrgPage() {
  const state = useProfile('orgs', '/orgs/', describe);
  // "Drilled" the organisation is the show Drilled: its page is the show's.
  const show = state.data?.is_show;
  useEffect(() => {
    if (show) window.location.replace(showHref(show.podcast_id, show.slug));
  }, [show]);
  if (show) return null;
  return (
    <ProfileLayout state={state} kindLabel="Organisation">
      {(o) => {
        const where = [o.hq_city, o.country].filter(Boolean).join(', ');
        const current = o.people.filter(p => !p.former);
        const former = o.people.filter(p => p.former);
        const person = (p) => (
          <li key={p.host_id} className="py-2 flex items-center gap-3">
            <img src={p.profile_image_url ? imageUrl(p.profile_image_url, 80) : avatarUrl(p.name)} alt=""
              onError={imageFallback(p.profile_image_url, p.name)}
              className="w-9 h-9 rounded-full object-cover bg-gray-100 flex-shrink-0" />
            <div className="flex-1 min-w-0">
              <a href={personHref(p.host_id, p.slug)} className="text-sm font-medium text-gray-900 hover:text-teal-700 hover:underline">{p.name}</a>
              <p className="text-xs text-gray-500 truncate">
                {[p.title, p.org_name !== o.name ? p.org_name : null].filter(Boolean).join(', ')}
              </p>
            </div>
            <span className="text-xs text-gray-400 flex-shrink-0">{plural(p.appearances, 'episode')} · {fmtMonthYear(p.last_date)}</span>
            <AdminEditLink href={`/admin/people?host_id=${p.host_id}`} className="flex-shrink-0">edit</AdminEditLink>
          </li>
        );
        return (
          <>
            <section className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6 flex flex-col sm:flex-row gap-4 sm:gap-6 items-center sm:items-start">
              <OrgLogo orgId={o.org_id} name={o.name} size={96} className="rounded-xl" />
              <div className="flex-1 min-w-0 text-center sm:text-left">
                <h1 className="text-2xl font-bold text-gray-900">{o.name}</h1>
                <AdminEditLink href={`/admin/companies?org_id=${o.org_id}`} />
                <p className="mt-1 text-sm text-gray-600 flex flex-wrap justify-center sm:justify-start items-center gap-x-2 gap-y-1">
                  {o.org_type && (
                    <span className="inline-flex items-center gap-1">
                      <span className="w-2.5 h-2.5 rounded-sm" style={{ background: ORG_TYPE_COLORS[o.org_type] }} />
                      {ORG_TYPE_LABELS[o.org_type] || o.org_type}
                    </span>
                  )}
                  {where && <span>· {where}</span>}
                  {o.founded_year && <span>· founded {o.founded_year}</span>}
                </p>
                {o.podcasts?.length > 0 && (
                  <p className="mt-1 text-sm text-gray-600 flex flex-wrap justify-center sm:justify-start items-center gap-x-2 gap-y-1">
                    {o.podcasts.length === 1 ? 'Podcast:' : 'Podcasts:'}
                    {o.podcasts.map(p => (
                      <a key={p.podcast_id} href={showHref(p.podcast_id, p.slug)} className="inline-flex items-center gap-1.5 text-teal-700 hover:underline">
                        <ShowThumb show={p} size="w-5 h-5" />{p.title}
                      </a>
                    ))}
                  </p>
                )}
                {o.parent && (
                  <p className="mt-1 text-sm text-gray-600">
                    Part of <a href={orgHref(o.parent.org_id, o.parent.slug)} className="text-teal-700 hover:underline">{o.parent.name}</a>
                  </p>
                )}
                <div className="mt-3 flex flex-wrap justify-center sm:justify-start gap-3 text-sm">
                  {o.website_url && <a href={o.website_url} target="_blank" rel="noopener noreferrer" className="text-teal-700 hover:underline">Website</a>}
                  {o.linkedin_url && <a href={o.linkedin_url} target="_blank" rel="noopener noreferrer" className="text-teal-700 hover:underline">LinkedIn</a>}
                  {o.wikipedia_url && <a href={o.wikipedia_url} target="_blank" rel="noopener noreferrer" className="text-teal-700 hover:underline">Wikipedia</a>}
                </div>
              </div>
            </section>

            {/* Only discussed (no guest has worked there): lead with that, not three zeros. */}
            {o.totals.people === 0 && o.discussed_in?.episodes > 0 ? (
              <div className="grid grid-cols-1 gap-2">
                <Stat label="Discussed on" value={plural(o.discussed_in.episodes, 'episode')} />
              </div>
            ) : (
              <div className="grid grid-cols-3 gap-2">
                <Stat label="People" value={o.totals.people} />
                <Stat label="Appearances" value={o.totals.appearances} />
                <Stat label="Shows" value={o.totals.shows} />
              </div>
            )}

            {o.people.length === 0 && (
              <p className="text-sm text-gray-500 text-center">No guests from {o.name} recorded yet.</p>
            )}

            {current.length > 0 && (
              <Section title={`People from ${o.name}`} aside={o.children.length ? 'including its sub-organisations' : undefined}>
                <ul className="divide-y divide-gray-100"><ShowMore items={current} initial={12} render={person} /></ul>
              </Section>
            )}
            <TalksAbout summary={o.talks_about} title="What its people talk about" noun="its people's" />

            {former.length > 0 && (
              <Section title="Formerly here">
                <ul className="divide-y divide-gray-100"><ShowMore items={former} initial={8} render={person} /></ul>
              </Section>
            )}

            <div className="grid sm:grid-cols-2 gap-5">
              {o.by_year.length > 0 && (
                <Section title="Airtime by year" aside="guest appearances">
                  <MiniBars rows={o.by_year} labelKey="year" valueKey="appearances" />
                </Section>
              )}
              {o.shows.length > 0 && (
                <Section title="Shows that book them most">
                  <ul className="space-y-2">
                    {o.shows.map(s => (
                      <li key={s.podcast_id} className="flex items-center gap-2 text-sm">
                        <ShowThumb show={s} size="w-7 h-7" />
                        <a href={showHref(s.podcast_id, s.slug)} className="flex-1 truncate text-gray-900 hover:text-teal-700 hover:underline">{s.title}</a>
                        <span className="text-xs text-gray-400 flex-shrink-0">{s.appearances}</span>
                      </li>
                    ))}
                  </ul>
                </Section>
              )}
            </div>

            {o.recent_episodes.length > 0 && (
              <Section title="Recent episodes">
                <ul className="divide-y divide-gray-100">
                  {o.recent_episodes.map(e => (
                    <li key={e.episode_id} className="py-2.5 flex items-start justify-between gap-3">
                      <ShowThumb show={e.show} />
                      <div className="min-w-0 flex-1">
                        <p className="text-sm font-medium text-gray-900">{e.title}</p>
                        <p className="text-xs text-gray-500 mt-0.5">
                          <a href={showHref(e.show.podcast_id, e.show.slug)} className="text-teal-700 hover:underline">{e.show.title}</a>
                          {' · '}{fmtDate(e.published_date)}{' · '}
                          {e.people.map((p, i) => (
                            <span key={p.host_id}>{i ? ', ' : ''}<a href={personHref(p.host_id, p.slug)} className="hover:underline">{p.name}</a></span>
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

            <DiscussedIn discussed={o.discussed_in} name={o.name} />

            {o.children.length > 0 && (
              <Section title="Includes">
                <div className="flex flex-wrap gap-2">
                  {o.children.map(c => (
                    <a key={c.org_id} href={orgHref(c.org_id, c.slug)}
                       className="inline-flex items-center gap-1.5 text-sm px-2 py-1 rounded-lg border border-gray-200 hover:border-teal-400">
                      <OrgLogo orgId={c.org_id} name={c.name} size={18} />{c.name}
                    </a>
                  ))}
                </div>
              </Section>
            )}
          </>
        );
      }}
    </ProfileLayout>
  );
}
