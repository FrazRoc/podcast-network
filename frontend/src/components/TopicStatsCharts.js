import { useState, useEffect } from 'react';
import { API_BASE_URL } from '../config';
import { CATEGORY_COLORS, CATEGORIES, Swatch } from './Topics';
import StackedShareBars from './StackedShareBars';
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

const W = 640;
const H = 260;
const PAD = { top: 18, right: 16, bottom: 30, left: 40 };

// One category's share of tagged episodes, year by year, on its own scale
// (like Guest Mix by Year). Policy first: it's the one that swings with
// elections and COPs.
export function TopicCategoriesByYearChart() {
  const { data, error } = useStat('/api/stats/topic-categories-by-year');
  const [focus, setFocus] = useState('Policy and politics');
  const [hover, setHover] = useState(null);
  if (!data) return <Status error={error} />;

  const items = data.items;
  const values = items.map(it => (100 * (it.counts[focus] || 0)) / (it.total || 1));
  const colour = CATEGORY_COLORS[focus];
  const top = Math.max(...values, 1);
  const step = top > 40 ? 10 : top > 16 ? 5 : 2;
  const peak = Math.ceil((top * 1.15) / step) * step;
  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;
  const INSET = 18;
  const xAt = i => PAD.left + INSET + (i / Math.max(1, items.length - 1)) * (plotW - 2 * INSET);
  const yAt = v => PAD.top + plotH - (v / peak) * plotH;
  const partialIdx = items.findIndex(it => it.year === data.partial_year);
  const line = values.map((v, i) => `${i ? 'L' : 'M'}${xAt(i).toFixed(1)} ${yAt(v).toFixed(1)}`).join(' ');
  const area = `${line} L${xAt(items.length - 1)} ${yAt(0)} L${xAt(0)} ${yAt(0)} Z`;
  const ticks = Array.from({ length: Math.floor(peak / step) + 1 }, (_, i) => i * step);

  return (
    <div>
      <div className="flex flex-wrap gap-1.5 mb-3">
        {data.categories.map(c => (
          <button key={c} onClick={() => setFocus(c)}
            className={`flex items-center gap-1.5 text-xs rounded px-2 py-0.5 ${focus === c ? 'bg-gray-900 text-white' : 'text-gray-600 hover:bg-gray-100'}`}>
            <Swatch category={c} className="w-2.5 h-2.5" />{c}
          </button>
        ))}
      </div>
      <div className="overflow-x-auto">
        <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W }} onMouseLeave={() => setHover(null)}>
          {ticks.map(v => (
            <g key={v}>
              <line x1={PAD.left} x2={PAD.left + plotW} y1={yAt(v)} y2={yAt(v)} stroke="#f3f4f6" />
              <text x={PAD.left - 6} y={yAt(v) + 3} fontSize={10} fill="#9ca3af" textAnchor="end">{v}%</text>
            </g>
          ))}
          <path d={area} fill={colour} opacity={0.15} />
          <path d={line} fill="none" stroke={colour} strokeWidth={2.5} />
          {values.map((v, i) => (
            <g key={i}>
              <circle cx={xAt(i)} cy={yAt(v)} r={hover === i ? 5 : 3.5} fill={colour} stroke="white" strokeWidth={1.5} />
              <text x={xAt(i)} y={yAt(v) - 9} fontSize={10} fill="#374151" textAnchor="middle">{v.toFixed(0)}%</text>
            </g>
          ))}
          {items.map((it, i) => (
            <g key={it.year}>
              <text x={xAt(i)} y={H - PAD.bottom + 16} fontSize={10} fill="#9ca3af" textAnchor="middle">
                {it.year}{i === partialIdx ? '*' : ''}
              </text>
              <rect x={xAt(i) - plotW / (items.length * 2)} y={PAD.top} width={plotW / items.length} height={plotH}
                fill="transparent" onMouseEnter={() => setHover(i)} />
            </g>
          ))}
        </svg>
      </div>
      <p className="text-xs text-gray-500 h-4 mt-1">
        {hover != null && `${items[hover].year}: ${items[hover].counts[focus] || 0} of ${items[hover].total} tagged episodes touched ${focus.toLowerCase()}.`}
      </p>
      <p className="text-xs text-gray-400 mt-1">
        An episode counts once for each category its topics fall in, so the categories add up to more than 100%.
        Years with fewer than 40 tagged episodes are left out. * part year.
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
  const { data, error } = useStat('/api/stats/rising-topics');
  if (!data) return <Status error={error} />;
  return (
    <div>
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
        and 3 different shows.
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
