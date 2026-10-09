import ProfileLayout, { Section, Stat, MiniBars } from './ProfileLayout';
import OrgLogo from './OrgLogo';
import AdminEditLink from './AdminEditLink';
import { ORG_TYPE_LABELS, ORG_TYPE_COLORS } from '../chartUtils';
import { stripHtmlForDisplay } from '../adminUtils';
import { useProfile, personHref, orgHref, showHref, avatarUrl, coverUrl, fmtDate, fmtMonthYear, plural, imageUrl, imageFallback } from '../profileUtils';
import { TalksAbout, EpisodeTopics, SimilarByTopic } from './Topics';

const describe = (s) => [s.title,
  `${s.title}: ${plural(s.totals.episodes, 'episode')} since ${fmtMonthYear(s.totals.first_date)}, who it books, `
  + 'its most frequent guests, and the shows that share its guests.'];

// Episodes per year from the monthly history.
const byYear = (history) => {
  const years = {};
  history.forEach(h => { const y = String(h.month).slice(0, 4); years[y] = (years[y] || 0) + h.n; });
  return Object.entries(years).map(([year, episodes]) => ({ year, episodes }));
};

function GuestMix({ mix }) {
  const entries = mix.types.map(t => [t, mix.counts[t] || 0]).filter(([, n]) => n > 0);
  const total = entries.reduce((s, [, n]) => s + n, 0);
  if (!total) return <p className="text-sm text-gray-500">Not enough guests with a known organisation yet.</p>;
  return (
    <div>
      <div className="flex h-4 rounded overflow-hidden">
        {entries.map(([t, n]) => (
          <div key={t} title={`${ORG_TYPE_LABELS[t]}: ${n}`} style={{ width: `${(100 * n) / total}%`, background: ORG_TYPE_COLORS[t] }} />
        ))}
      </div>
      <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1 text-xs text-gray-600">
        {entries.sort((a, b) => b[1] - a[1]).map(([t, n]) => (
          <span key={t} className="inline-flex items-center gap-1">
            <span className="w-2.5 h-2.5 rounded-sm" style={{ background: ORG_TYPE_COLORS[t] }} />
            {ORG_TYPE_LABELS[t]} {Math.round((100 * n) / total)}%
          </span>
        ))}
      </div>
      <p className="mt-1 text-[11px] text-gray-400">{plural(total, 'guest')} with a known organisation, by the kind of organisation they work for.</p>
    </div>
  );
}

export default function ShowPage() {
  const state = useProfile('shows', '/shows/', describe);
  return (
    <ProfileLayout state={state} kindLabel="Show">
      {(s) => {
        const t = s.totals;
        const coverage = t.guest_eligible ? Math.round((100 * t.with_guest) / t.guest_eligible) : null;
        return (
          <>
            <section className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6 flex flex-col sm:flex-row gap-4 sm:gap-6 items-center sm:items-start">
              <img src={s.cover_art_url ? coverUrl(s.cover_art_url, 300) : avatarUrl(s.title)} alt={s.title}
                onError={e => { e.target.onerror = null; e.target.src = avatarUrl(s.title); }}
                className="w-32 h-32 rounded-xl object-cover bg-gray-100 flex-shrink-0" />
              <div className="flex-1 min-w-0 text-center sm:text-left">
                <h1 className="text-2xl font-bold text-gray-900">{s.title}</h1>
                <div className="flex justify-center sm:justify-start gap-3">
                  {s.apple_podcast_id && <AdminEditLink href={`/admin/shows?apple_podcast_id=${s.apple_podcast_id}`} />}
                  <AdminEditLink href={`/admin/episodes?show=${encodeURIComponent(s.title)}`}>episodes in admin</AdminEditLink>
                </div>
                {s.publisher ? (
                  <p className="text-sm text-gray-500 mt-0.5 flex items-center justify-center sm:justify-start gap-1.5">
                    From <OrgLogo orgId={s.publisher.org_id} name={s.publisher.name} size={16} />
                    <a href={orgHref(s.publisher.org_id, s.publisher.slug)} className="text-teal-700 hover:underline">{s.publisher.name}</a>
                  </p>
                ) : (s.channel && s.channel !== s.title && <p className="text-sm text-gray-500 mt-0.5">{s.channel}</p>)}
                {s.hosts.length > 0 && (
                  <p className="mt-2 text-sm text-gray-700">
                    Hosted by{' '}
                    {s.hosts.slice(0, 6).map((h, i) => (
                      <span key={h.host_id}>{i ? ', ' : ''}<a href={personHref(h.host_id, h.slug)} className="text-teal-700 hover:underline">{h.name}</a></span>
                    ))}
                    {s.hosts.length > 6 && ` and ${s.hosts.length - 6} more`}
                  </p>
                )}
                {s.description && (
                  <p className="mt-3 text-sm text-gray-600 leading-relaxed line-clamp-4">{stripHtmlForDisplay(s.description)}</p>
                )}
                <div className="mt-3 flex flex-wrap justify-center sm:justify-start gap-3 text-sm">
                  {s.apple_url && <a href={s.apple_url} target="_blank" rel="noopener noreferrer" className="text-teal-700 hover:underline">Apple Podcasts</a>}
                  {s.website_url && <a href={s.website_url} target="_blank" rel="noopener noreferrer" className="text-teal-700 hover:underline">Website</a>}
                  {s.rss_feed_url && <a href={s.rss_feed_url} target="_blank" rel="noopener noreferrer" className="text-teal-700 hover:underline">RSS</a>}
                </div>
              </div>
            </section>

            <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
              <Stat label="Episodes" value={t.episodes} />
              <Stat label="Since" value={fmtMonthYear(t.first_date) || '—'} />
              <Stat label="Per month" value={t.per_month_last_year} />
              <Stat label="Guest named" value={coverage === null ? '—' : `${coverage}%`} />
            </div>
            <p className="text-[11px] text-gray-400 -mt-3">
              Per month: average over the last 12 months. Guest named: share of episodes with a guest
              (episodes known to have none, like solo essays, aren't counted).
            </p>

            <Section title="Who they book">
              <GuestMix mix={s.guest_mix} />
            </Section>

            <TalksAbout summary={s.covers} title="What it covers" noun="its" />
            <SimilarByTopic kind="shows" id={s.podcast_id} title="Shows that cover similar things" />

            <div className="grid sm:grid-cols-2 gap-5">
              {s.top_guests.length > 0 && (
                <Section title="Most frequent guests">
                  <ul className="space-y-2">
                    {s.top_guests.map(g => (
                      <li key={g.host_id} className="flex items-center gap-2 text-sm">
                        <img src={g.profile_image_url ? imageUrl(g.profile_image_url, 64) : avatarUrl(g.name)} alt=""
                          onError={imageFallback(g.profile_image_url, g.name)}
                          className="w-7 h-7 rounded-full object-cover bg-gray-100" />
                        <a href={personHref(g.host_id, g.slug)} className="flex-1 truncate text-gray-900 hover:text-teal-700 hover:underline">{g.name}</a>
                        <span className="text-xs text-gray-400">{plural(g.appearances, 'episode')}</span>
                      </li>
                    ))}
                  </ul>
                </Section>
              )}
              {s.top_orgs.length > 0 && (
                <Section title="Organisations they book most" aside="distinct guests">
                  <ul className="space-y-2">
                    {s.top_orgs.map(o => (
                      <li key={o.org_id} className="flex items-center gap-2 text-sm">
                        <OrgLogo orgId={o.org_id} name={o.name} size={24} />
                        <a href={orgHref(o.org_id, o.slug)} className="flex-1 truncate text-gray-900 hover:text-teal-700 hover:underline">{o.name}</a>
                        <span className="text-xs text-gray-400">{o.guests}</span>
                      </li>
                    ))}
                  </ul>
                </Section>
              )}
            </div>

            <Section title="Recent episodes">
              <ul className="divide-y divide-gray-100">
                {s.recent_episodes.map(e => (
                  <li key={e.episode_id} className="py-2.5 flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <p className="text-sm font-medium text-gray-900">{e.title}</p>
                      <p className="text-xs text-gray-500 mt-0.5">
                        {fmtDate(e.published_date)}
                        {e.guests.length > 0 && ' · with '}
                        {e.guests.map((g, i) => (
                          <span key={g.host_id}>{i ? ', ' : ''}<a href={personHref(g.host_id, g.slug)} className="text-teal-700 hover:underline">{g.name}</a></span>
                        ))}
                      </p>
                      <EpisodeTopics topics={e.topics} companies={e.companies} people={e.people_mentioned} />
                    </div>
                    <AdminEditLink href={`/admin/episodes?episode_id=${e.episode_id}`} className="flex-shrink-0">edit</AdminEditLink>
                    {e.listen_url && <a href={e.listen_url} target="_blank" rel="noopener noreferrer" className="flex-shrink-0 text-xs text-teal-700 hover:underline">Listen ↗</a>}
                  </li>
                ))}
              </ul>
            </Section>

            <div className="grid sm:grid-cols-2 gap-5">
              {s.history.length > 0 && (
                <Section title="Episodes per year">
                  <MiniBars rows={byYear(s.history)} labelKey="year" valueKey="episodes" />
                </Section>
              )}
              {s.overlap.length > 0 && (
                <Section title="Shows that share its guests">
                  <ul className="space-y-2">
                    {s.overlap.map(x => (
                      <li key={x.podcast_id} className="flex items-center gap-2 text-sm">
                        <img src={x.cover_art_url ? coverUrl(x.cover_art_url, 80) : avatarUrl(x.title)} alt="" className="w-7 h-7 rounded object-cover bg-gray-100" />
                        <a href={showHref(x.podcast_id, x.slug)} className="flex-1 truncate text-gray-900 hover:text-teal-700 hover:underline">{x.title}</a>
                        <span className="text-xs text-gray-400">{plural(x.shared, 'shared guest')}</span>
                      </li>
                    ))}
                  </ul>
                </Section>
              )}
            </div>
          </>
        );
      }}
    </ProfileLayout>
  );
}
