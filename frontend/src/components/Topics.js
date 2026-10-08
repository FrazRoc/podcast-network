import { Section, ShowMore, ShowThumb } from './ProfileLayout';
import { topicHref, orgHref, personHref, showHref, fmtDate, plural } from '../profileUtils';

// Shared topic pieces: the colour for each of the 12 fixed categories
// (backend/topic_names.py CATEGORIES), a topic chip, the "Talks about" block
// on profiles, and the small topic line under an episode.

export const CATEGORY_COLORS = {
  'Power generation': '#f28e2b',
  'Grid and storage': '#4e79a7',
  'Transport': '#76b7b2',
  'Buildings': '#9c755f',
  'Industry and materials': '#7f7f7f',
  'Fuels': '#e15759',
  'Carbon and land': '#59a14f',
  'Policy and politics': '#b07aa1',
  'Finance and markets': '#edc948',
  'Tech and AI': '#499894',
  'Climate science and impacts': '#ff9da7',
  'Society and justice': '#8cd17d',
};
export const CATEGORIES = Object.keys(CATEGORY_COLORS);
export const categoryColor = (c) => CATEGORY_COLORS[c] || '#bab0ac';

export const Swatch = ({ category, className = 'w-2 h-2' }) => (
  <span className={`${className} rounded-sm flex-shrink-0 inline-block`} style={{ background: categoryColor(category) }} />
);

// A link to a topic's page; `count` is shown after the name.
export function TopicChip({ topic, count, size = 'sm' }) {
  const pad = size === 'xs' ? 'px-2 py-0.5 text-[11px]' : 'px-2.5 py-1 text-xs';
  return (
    <a href={topicHref(topic.tag_id, topic.slug)} title={topic.category}
      className={`inline-flex items-center gap-1.5 whitespace-nowrap rounded-full border border-gray-200 bg-white ${pad} text-gray-700 hover:border-teal-500 hover:text-teal-800`}>
      <Swatch category={topic.category} />
      {topic.name}
      {count != null && <span className="text-gray-400">{count}</span>}
    </a>
  );
}

// The topics under one episode, main topic first.
// Companies and people the episode discusses follow, in grey: they aren't
// topics, and link to the organisation's or person's page where there is one.
const MENTION = 'inline-flex items-center whitespace-nowrap rounded-full bg-gray-100 px-2 py-0.5 text-[11px] text-gray-600';
const Mention = ({ href, label, name }) => (href
  ? <a href={href} title={label} className={`${MENTION} hover:bg-gray-200 hover:text-gray-900`}>{name}</a>
  : <span title={label} className={MENTION}>{name}</span>);

export function EpisodeTopics({ topics, companies, people }) {
  if (!topics?.length && !companies?.length && !people?.length) return null;
  return (
    <div className="mt-1 flex flex-wrap gap-1">
      {(topics || []).map(t => <TopicChip key={t.tag_id} topic={t} size="xs" />)}
      {(people || []).map(m => <Mention key={m.tag_id} label="Person" name={m.name}
        href={m.host_id ? personHref(m.host_id, m.slug) : null} />)}
      {(companies || []).map(c => <Mention key={c.tag_id} label="Company" name={c.name}
        href={c.org_id ? orgHref(c.org_id, c.slug) : null} />)}
    </div>
  );
}

// "Episodes that discuss X" on an organisation's or person's page: episodes
// tagged with them as a subject, whether or not they were on.
export function DiscussedIn({ discussed, name }) {
  if (!discussed?.episodes) return null;
  const { episodes, recent } = discussed;
  return (
    <Section title={`Episodes that discuss ${name}`}
      aside={episodes > recent.length ? `latest ${recent.length} of ${episodes}` : undefined}>
      <ul className="divide-y divide-gray-100">
        <ShowMore items={recent} initial={8} render={e => (
          <li key={e.episode_id} className="py-2.5 flex items-start justify-between gap-3">
            <ShowThumb show={e.show} />
            <div className="min-w-0 flex-1">
              <p className="text-sm font-medium text-gray-900">{e.title}</p>
              <p className="text-xs text-gray-500 mt-0.5">
                <a href={showHref(e.show.podcast_id, e.show.slug)} className="text-teal-700 hover:underline">{e.show.title}</a>
                {' · '}{fmtDate(e.published_date)}
              </p>
            </div>
            {e.listen_url && <a href={e.listen_url} target="_blank" rel="noopener noreferrer" className="flex-shrink-0 text-xs text-teal-700 hover:underline">Listen ↗</a>}
          </li>
        )} />
      </ul>
    </Section>
  );
}

// "Talks about" on a person or organisation page, "What it covers" on a
// show: the topics on 2+ of their tagged episodes, and the category mix.
// Topics cover only part of the archive so far, so it says what the counts
// are out of; with too little tagged it says so instead of guessing.
// Broad areas first (each with the share of tagged episodes under it),
// then the specific topics, then the category mix.
export function TalksAbout({ summary, title = 'Talks about', noun = 'their' }) {
  if (!summary || !summary.tagged_episodes) return null;
  const { topics, categories, tagged_episodes: tagged } = summary;
  const areas = summary.areas || [];
  const total = categories.reduce((n, c) => n + c.episodes, 0);
  return (
    <Section title={title} aside={`from ${plural(tagged, 'tagged episode')}`}>
      {areas.length > 0 && (
        <div className="mb-3">
          <p className="text-[11px] font-medium uppercase tracking-wide text-gray-400 mb-1.5">Areas</p>
          <div className="flex flex-wrap gap-1.5">
            {areas.map(a => <TopicChip key={a.tag_id} topic={a} count={`${a.share}%`} />)}
          </div>
        </div>
      )}
      {areas.length > 0 && topics.length > 0 && (
        <p className="text-[11px] font-medium uppercase tracking-wide text-gray-400 mb-1.5">Topics</p>
      )}
      {topics.length > 0 ? (
        <div className="flex flex-wrap gap-1.5">
          {topics.map(t => <TopicChip key={t.tag_id} topic={t} count={t.episodes} />)}
        </div>
      ) : (
        <p className="text-sm text-gray-500">
          No topic comes up on more than one of {noun} tagged episodes yet.
        </p>
      )}
      {categories.length > 0 && (
        <div className="mt-4">
          <div className="flex h-2.5 rounded-full overflow-hidden bg-gray-100">
            {categories.map(c => (
              <div key={c.category} title={`${c.category}: ${plural(c.episodes, 'episode')}`}
                style={{ width: `${(100 * c.episodes) / total}%`, background: categoryColor(c.category) }} />
            ))}
          </div>
          <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1 text-[11px] text-gray-500">
            {categories.map(c => (
              <span key={c.category} className="inline-flex items-center gap-1">
                <Swatch category={c.category} />{c.category} <span className="text-gray-400">{c.episodes}</span>
              </span>
            ))}
          </div>
        </div>
      )}
      <p className="mt-3 text-[11px] text-gray-400">
        Read from episode descriptions. An area's share is the tagged episodes with any topic under it.
      </p>
    </Section>
  );
}
