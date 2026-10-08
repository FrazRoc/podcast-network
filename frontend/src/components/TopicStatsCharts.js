import { useState, useEffect } from 'react';
import { API_BASE_URL } from '../config';
import { CATEGORY_COLORS, CATEGORIES, Swatch } from './Topics';
import StackedShareBars from './StackedShareBars';
import ShareByYearChart from './ShareByYearChart';
import { showHref, orgHref, personHref, topicHref, slugify } from '../profileUtils';

// The Stats page's Topics tab. Topics cover a random sample of episodes, so
// every chart here is a share of tagged episodes rather than a raw count.

function useStat(path) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  useEffect(() => {
    setData(null);
    fetch(`${API_BASE_URL}${path}`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, [path]);
  return { data, error };
}

function Status({ error }) {
  return error ? <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>
    : <p className="text-sm text-gray-400">Loading…</p>;
}

const CATEGORY_LABELS = Object.fromEntries(CATEGORIES.map(c => [c, c]));

// Share of tagged episodes touching each topic category, per year. All
// categories together by default; pick one to see it on its own scale.
export function TopicCategoriesByYearChart() {
  const { data, error } = useStat('/api/stats/topic-categories-by-year');
  if (!data) return <Status error={error} />;
  return (
    <ShareByYearChart items={data.items} keys={data.categories} colors={CATEGORY_COLORS} labels={CATEGORY_LABELS}
      partialYear={data.partial_year}
      share={(it, c) => (100 * (it.counts[c] || 0)) / (it.total || 1)}
      describeYear={(it, c) => `${it.year}: ${it.counts[c] || 0} of ${it.total} tagged episodes touched ${c.toLowerCase()}.`}
      footnote="An episode counts once for each category its topics fall in, so the categories add up to more than 100%. Years with fewer than 40 tagged episodes are left out. * part year." />
  );
}

// Distinct line colours for the few broad topics in one category.
const AREA_PALETTE = ['#4e79a7', '#f28e2b', '#e15759', '#59a14f', '#b07aa1', '#76b7b2', '#edc948', '#ff9da7', '#9c755f'];

// Share of tagged episodes touching each broad topic, per year — one
// category's broad topics at a time (55 lines at once would be unreadable).
export function TopicAreasByYearChart() {
  const { data, error } = useStat('/api/stats/topic-areas-by-year');
  const [category, setCategory] = useState('Grid and storage');
  if (!data) return <Status error={error} />;
  const inCat = data.broad.filter(b => b.category === category);
  const keys = inCat.map(b => b.key);
  const colors = Object.fromEntries(inCat.map((b, i) => [b.key, AREA_PALETTE[i % AREA_PALETTE.length]]));
  const labels = Object.fromEntries(inCat.map(b => [b.key, b.name]));
  const cats = CATEGORIES.filter(c => data.broad.some(b => b.category === c));
  return (
    <div>
      <label className="flex items-center gap-2 text-xs text-gray-500 mb-3">
        <Swatch category={category} className="w-2.5 h-2.5" />
        <select value={category} onChange={e => setCategory(e.target.value)}
          className="border border-gray-200 rounded px-2 py-1 text-gray-700 bg-white">
          {cats.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
      </label>
      <ShareByYearChart key={category} items={data.items} keys={keys} colors={colors} labels={labels}
        partialYear={data.partial_year}
        share={(it, k) => (100 * (it.counts[k] || 0)) / (it.total || 1)}
        describeYear={(it, k) => `${it.year}: ${it.counts[k] || 0} of ${it.total} tagged episodes touched ${labels[k]}.`}
        footnote="An episode counts once for each broad topic any of its topics sit under. Years with fewer than 40 tagged episodes are left out. * part year." />
      <p className="text-xs text-gray-400 mt-1">
        {inCat.map((b, i) => (
          <span key={b.key}>{i ? ' · ' : 'Topic pages: '}<a href={topicHref(b.tag_id, b.slug)} className="hover:underline">{b.name}</a></span>
        ))}
      </p>
    </div>
  );
}

function TopicMoves({ rows, cutoff, direction }) {
  const max = Math.max(...rows.map(r => Math.max(r.recent_share, r.earlier_share)), 0.1);
  return (
    <ul className="space-y-2">
      {rows.map(r => (
        <li key={r.tag_id} className="text-xs">
          <div className="flex items-center gap-1.5">
            <Swatch category={r.category} />
            <a href={topicHref(r.tag_id, r.slug)} className="flex-1 truncate text-gray-800 hover:text-teal-700 hover:underline">{r.name}</a>
            <span className="tabular-nums text-gray-400">{r.earlier_share.toFixed(1)}% → {r.recent_share.toFixed(1)}%</span>
          </div>
          <div className="mt-0.5 ml-3.5 space-y-px" title={`Before ${cutoff}: ${r.earlier} episodes · since: ${r.recent}`}>
            <div className="h-1.5 rounded-sm bg-gray-300" style={{ width: `${(100 * r.earlier_share) / max}%` }} />
            <div className="h-1.5 rounded-sm" style={{ width: `${(100 * r.recent_share) / max}%`,
              background: direction === 'up' ? '#0d9488' : '#e15759' }} />
          </div>
        </li>
      ))}
    </ul>
  );
}

// The topics whose share of episodes moved most between the last two years
// and everything before.
export function RisingTopicsChart() {
  const [level, setLevel] = useState('broad');
  const { data, error } = useStat(`/api/stats/rising-topics?level=${level}`);
  const toggle = (
    <div className="flex rounded-lg bg-gray-200/70 p-0.5 text-xs max-w-[16rem] mb-3">
      {[['broad', 'Broad topics'], ['topic', 'Topics']].map(([k, label]) => (
        <button key={k} onClick={() => setLevel(k)}
          className={`flex-1 rounded-md py-1 font-medium ${level === k ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}>
          {label}
        </button>
      ))}
    </div>
  );
  if (!data || (data.level && data.level !== level)) return <div>{toggle}<Status error={error} /></div>;
  return (
    <div>
      {toggle}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
        <div>
          <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide mb-2">Rising</p>
          <TopicMoves rows={data.rising} cutoff={data.cutoff_year} direction="up" />
        </div>
        <div>
          <p className="text-xs font-semibold text-red-600 uppercase tracking-wide mb-2">Fading</p>
          <TopicMoves rows={data.falling} cutoff={data.cutoff_year} direction="down" />
        </div>
      </div>
      <p className="text-xs text-gray-400 mt-3">
        Share of tagged episodes before {data.cutoff_year} (grey, {data.earlier_episodes} episodes) against
        {' '}{data.cutoff_year} onward (colour, {data.recent_episodes} episodes). Topics on at least 8 episodes
        and 3 different shows{level === 'broad' && '; a broad topic counts every topic under it'}.
      </p>
    </div>
  );
}

// Each show's tagged episodes by topic category.
export function ShowTopicMixChart() {
  const { data, error } = useStat('/api/stats/show-topic-mix');
  if (!data) return <Status error={error} />;
  const rows = data.items.map(s => ({ id: s.podcast_id, label: s.title, counts: s.counts, total: s.total,
                                      href: showHref(s.podcast_id, slugify(s.title)) }));
  return (
    <>
      <StackedShareBars rows={rows} keys={data.categories} colors={CATEGORY_COLORS} labels={CATEGORY_LABELS}
        noun="episode-category pairs" />
      <p className="text-xs text-gray-400 mt-3">
        Shows with at least 25 tagged episodes, most tagged first. An episode counts once in each category its
        topics fall in; the number is those pairs.
      </p>
    </>
  );
}

// The companies and people most discussed on episodes they weren't on.
export function MostDiscussedChart() {
  const [kind, setKind] = useState('company');
  const { data, error } = useStat(`/api/stats/most-discussed?kind=${kind}`);
  const href = (r) => (r.link_id ? (kind === 'person' ? personHref(r.link_id, r.slug) : orgHref(r.link_id, r.slug)) : null);
  return (
    <div>
      <div className="flex rounded-lg bg-gray-200/70 p-0.5 text-xs max-w-[14rem] mb-3">
        {[['company', 'Companies'], ['person', 'People']].map(([k, label]) => (
          <button key={k} onClick={() => setKind(k)}
            className={`flex-1 rounded-md py-1 font-medium ${kind === k ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}>
            {label}
          </button>
        ))}
      </div>
      {!data || data.kind !== kind ? <Status error={error} /> : (
        <>
          <ul className="space-y-1">
            {data.items.map((r, i) => {
              const max = data.items[0]?.episodes || 1;
              const link = href(r);
              return (
                <li key={`${r.name}-${i}`} className="flex items-center gap-2 text-xs">
                  <span className="w-36 sm:w-44 flex-shrink-0 truncate text-right text-gray-700" title={r.name}>
                    {link ? <a href={link} className="hover:text-teal-700 hover:underline">{r.name}</a> : r.name}
                  </span>
                  <div className="flex-1 h-4 bg-gray-100 rounded overflow-hidden">
                    <div className="h-full rounded bg-teal-600" style={{ width: `${(100 * r.episodes) / max}%` }} />
                  </div>
                  <span className="w-8 flex-shrink-0 text-gray-400 tabular-nums">{r.episodes}</span>
                </li>
              );
            })}
          </ul>
          <p className="text-xs text-gray-400 mt-3">
            Tagged episodes that discuss them, out of {data.tagged_episodes.toLocaleString()} tagged so far.
            Episodes they're on themselves don't count: the person credited, or a guest who works at the company.
          </p>
        </>
      )}
    </div>
  );
}
