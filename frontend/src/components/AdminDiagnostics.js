import { useState, useEffect, useCallback, useMemo } from 'react';
import { API_BASE_URL } from '../config';
import { adminFetch } from '../adminAuth';
import AdminHeader from './AdminHeader';

const API = `${API_BASE_URL}/api/admin`;

const pct = (part, whole) => (whole > 0 ? Math.round((100 * part) / whole) : 0);

// One hue, light to dark, for magnitude. Low coverage is the problem, so the
// bar is drawn from the deficit rather than the achievement.
const teal = (t) => `rgb(${Math.round(204 - 191 * t)}, ${Math.round(231 - 111 * t)}, ${Math.round(228 - 90 * t)})`;

function Bar({ value, max, color, title }) {
  const w = max > 0 ? (100 * value) / max : 0;
  return (
    <div className="h-2.5 bg-gray-100 rounded-sm overflow-hidden" title={title}>
      <div className="h-full rounded-sm" style={{ width: `${w}%`, background: color }} />
    </div>
  );
}

function SortHeader({ label, field, sort, setSort, align = 'right' }) {
  const active = sort.field === field;
  return (
    <th
      onClick={() => setSort({ field, dir: active && sort.dir === 'asc' ? 'desc' : 'asc' })}
      className={`px-2 py-2 font-medium cursor-pointer select-none whitespace-nowrap
                  ${align === 'right' ? 'text-right' : 'text-left'}
                  ${active ? 'text-gray-900' : 'text-gray-400 hover:text-gray-600'}`}
    >
      {label}{active ? (sort.dir === 'asc' ? ' ↑' : ' ↓') : ''}
    </th>
  );
}

// What "coverage" means in the main table — which of these an episode needs
// at least one of to count as covered, and how to talk about it.
const COVERAGE_METRICS = {
  any:   { label: 'Any credit',   key: 'episodes_with_credit', colLabel: 'Episodes credited',       noun: 'a credit' },
  host:  { label: 'Host credit',  key: 'episodes_with_host',   colLabel: 'Episodes with a host',     noun: 'a host' },
  guest: { label: 'Guest credit', key: 'episodes_with_guest',  colLabel: 'Episodes with a guest',    noun: 'a guest' },
};

export default function AdminDiagnostics() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [sort, setSort] = useState({ field: 'coverage', dir: 'asc' });
  const [minEpisodes, setMinEpisodes] = useState(20);
  const [metric, setMetric] = useState('any');

  const load = useCallback(async () => {
    try {
      const res = await adminFetch(`${API}/diagnostics`);
      if (!res.ok) throw new Error(`API error ${res.status}`);
      setData(await res.json());
    } catch (e) {
      setError(e.message || 'Failed to load');
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const shows = useMemo(() => {
    if (!data) return [];
    const coverageKey = COVERAGE_METRICS[metric].key;
    const rows = data.shows
      .filter(s => s.episodes >= minEpisodes)
      .map(s => ({
        ...s,
        coverage: pct(s[coverageKey], s.episodes),
        coverageCount: s[coverageKey],
        pctGuest: pct(s.guest_credits, s.credits),
        pctApple: pct(s.apple_credits, s.credits),
      }));
    const dir = sort.dir === 'asc' ? 1 : -1;
    return rows.sort((a, b) => {
      const av = a[sort.field], bv = b[sort.field];
      if (typeof av === 'string') return dir * av.localeCompare(bv);
      return dir * (av - bv);
    });
  }, [data, sort, minEpisodes, metric]);

  const perEpisode = data?.credits_per_episode || [];
  const maxBucket = Math.max(1, ...perEpisode.map(b => b.episodes));
  const totalEpisodes = perEpisode.reduce((n, b) => n + b.episodes, 0);

  // A show credited as all-guest with nobody registered as its host is not a
  // finding about the show; it means we never recorded who presents it.
  const missingHosts = shows.filter(s => s.credits >= 20 && s.pctGuest >= 85 && s.registered_hosts === 0);
  // Shows whose descriptions we deliberately don't read are expected to be
  // thin, so they belong outside the list of things to look into.
  const uncovered = shows.filter(s => s.coverage <= 20 && s.scan_descriptions !== false);

  return (
    <div className="min-h-screen bg-gray-100 font-sans">
      <AdminHeader
        active="Diagnostics"
        right={data && (
          <span className="text-sm text-gray-500">
            {data.totals.episodes.toLocaleString()} episodes · {data.totals.credits.toLocaleString()} credits · {data.totals.people.toLocaleString()} people
          </span>
        )}
      />

      <div className="max-w-6xl mx-auto p-4 sm:p-6 space-y-6">
        {error && <p className="text-sm text-red-500">Couldn't load: {error}</p>}
        {!data && !error && <p className="text-sm text-gray-400">Loading…</p>}

        {data && (
          <>
            {(uncovered.length > 0 || missingHosts.length > 0) && (
              <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-5">
                <h2 className="text-base font-semibold text-gray-900 mb-3">Worth looking at</h2>
                <div className="grid gap-4 sm:grid-cols-2 text-sm">
                  <div>
                    <p className="text-gray-500 mb-1">
                      {uncovered.length} show{uncovered.length === 1 ? '' : 's'} with 20% or less of episodes credited
                    </p>
                    <ul className="text-gray-800 space-y-0.5">
                      {uncovered.slice(0, 6).map(s => (
                        <li key={s.podcast_id}>
                          {s.title} <span className="text-gray-400">— {s.coverage}% of {s.episodes.toLocaleString()}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                  <div>
                    <p className="text-gray-500 mb-1">
                      {missingHosts.length} show{missingHosts.length === 1 ? '' : 's'} credited as nearly all guests, with no host registered
                    </p>
                    <ul className="text-gray-800 space-y-0.5">
                      {missingHosts.slice(0, 6).map(s => (
                        <li key={s.podcast_id}>
                          {s.title} <span className="text-gray-400">— {s.pctGuest}% guest</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              </div>
            )}

            <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
              <div className="flex flex-wrap items-baseline justify-between gap-2 mb-1">
                <h2 className="text-base font-semibold text-gray-900">Coverage and confidence by show</h2>
                <div className="flex items-center gap-3">
                  <div className="flex text-xs border border-gray-300 rounded overflow-hidden">
                    {Object.entries(COVERAGE_METRICS).map(([key, m]) => (
                      <button
                        key={key}
                        onClick={() => setMetric(key)}
                        className={`px-2 py-1 ${metric === key
                          ? 'bg-teal-600 text-white'
                          : 'bg-white text-gray-500 hover:bg-gray-50'}`}
                      >
                        {m.label}
                      </button>
                    ))}
                  </div>
                  <label className="text-xs text-gray-500">
                    min episodes{' '}
                    <select
                      value={minEpisodes}
                      onChange={e => setMinEpisodes(Number(e.target.value))}
                      className="border border-gray-300 rounded px-1 py-0.5"
                    >
                      {[0, 20, 50, 100].map(n => <option key={n} value={n}>{n}</option>)}
                    </select>
                  </label>
                </div>
              </div>
              <p className="text-sm text-gray-500 mb-4">
                {metric === 'any'
                  ? 'How much of each show is credited at all, and what those credits rest on.'
                  : `How many episodes are missing ${COVERAGE_METRICS[metric].noun === 'a host' ? 'any host credit' : 'any guest credit'}.`}
                {' '}Apple states its credits; everything else is inferred from the episode text.
              </p>

              <div className="overflow-x-auto">
                <table className="w-full text-sm border-collapse">
                  <thead>
                    <tr className="border-b border-gray-200 text-xs">
                      <SortHeader label="Show" field="title" sort={sort} setSort={setSort} align="left" />
                      <SortHeader label="Episodes" field="episodes" sort={sort} setSort={setSort} />
                      <th className="px-2 py-2 text-left font-medium text-gray-400 w-40">{COVERAGE_METRICS[metric].colLabel}</th>
                      <SortHeader label="%" field="coverage" sort={sort} setSort={setSort} />
                      <th className="px-2 py-2 text-left font-medium text-gray-400 w-40">From Apple</th>
                      <SortHeader label="%" field="pctApple" sort={sort} setSort={setSort} />
                      <SortHeader label="Credits" field="credits" sort={sort} setSort={setSort} />
                    </tr>
                  </thead>
                  <tbody>
                    {shows.map(s => (
                      <tr key={s.podcast_id} className="border-b border-gray-100 hover:bg-gray-50">
                        <td className="px-2 py-1.5 text-gray-900">
                          {s.title}
                          {s.scan_descriptions === false && (
                            <span className="ml-2 text-[10px] px-1.5 py-0.5 rounded-full bg-gray-100 text-gray-500 align-middle"
                                  title="Descriptions are deliberately not scanned for this show, so low coverage is expected">
                              titles only
                            </span>
                          )}
                        </td>
                        <td className="px-2 py-1.5 text-right text-gray-500 tabular-nums">{s.episodes.toLocaleString()}</td>
                        <td className="px-2 py-1.5">
                          <Bar value={s.coverage} max={100} color={teal(s.coverage / 100)}
                               title={`${s.coverageCount} of ${s.episodes} episodes have ${COVERAGE_METRICS[metric].noun}`} />
                        </td>
                        <td className={`px-2 py-1.5 text-right tabular-nums ${
                          s.coverage <= 20 && s.scan_descriptions !== false ? 'text-red-600 font-medium' : 'text-gray-600'}`}>
                          {s.coverage}%
                        </td>
                        <td className="px-2 py-1.5">
                          <Bar value={s.pctApple} max={100} color={teal(s.pctApple / 100)}
                               title={`${s.apple_credits} of ${s.credits} credits from Apple`} />
                        </td>
                        <td className="px-2 py-1.5 text-right text-gray-600 tabular-nums">{s.pctApple}%</td>
                        <td className="px-2 py-1.5 text-right text-gray-500 tabular-nums">{s.credits.toLocaleString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
              <h2 className="text-base font-semibold text-gray-900 mb-1">Host and guest balance</h2>
              <p className="text-sm text-gray-500 mb-4">
                A show credited as almost entirely guests usually means nobody was recorded as
                presenting it, rather than that it has no host.
              </p>
              <div className="overflow-x-auto">
                <table className="w-full text-sm border-collapse">
                  <thead>
                    <tr className="border-b border-gray-200 text-xs">
                      <SortHeader label="Show" field="title" sort={sort} setSort={setSort} align="left" />
                      <SortHeader label="Credits" field="credits" sort={sort} setSort={setSort} />
                      <th className="px-2 py-2 text-left font-medium text-gray-400 w-56">Host / guest split</th>
                      <SortHeader label="% guest" field="pctGuest" sort={sort} setSort={setSort} />
                      <SortHeader label="Hosts registered" field="registered_hosts" sort={sort} setSort={setSort} />
                    </tr>
                  </thead>
                  <tbody>
                    {shows.filter(s => s.credits > 0).map(s => (
                      <tr key={s.podcast_id} className="border-b border-gray-100 hover:bg-gray-50">
                        <td className="px-2 py-1.5 text-gray-900">{s.title}</td>
                        <td className="px-2 py-1.5 text-right text-gray-500 tabular-nums">{s.credits.toLocaleString()}</td>
                        <td className="px-2 py-1.5">
                          {/* Two hues here because the split is a comparison of
                              two named things, not a magnitude. */}
                          <div className="flex h-2.5 rounded-sm overflow-hidden bg-gray-100"
                               title={`${s.credits - s.guest_credits} host, ${s.guest_credits} guest`}>
                            <div style={{ width: `${100 - s.pctGuest}%`, background: '#0d9488' }} />
                            <div style={{ width: `${s.pctGuest}%`, background: '#cbd5e1' }} />
                          </div>
                        </td>
                        <td className="px-2 py-1.5 text-right text-gray-600 tabular-nums">{s.pctGuest}%</td>
                        <td className={`px-2 py-1.5 text-right tabular-nums ${
                          s.registered_hosts === 0 ? 'text-red-600 font-medium' : 'text-gray-500'}`}>
                          {s.registered_hosts}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="flex items-center gap-4 mt-3 text-xs text-gray-500">
                <span className="flex items-center gap-1.5"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: '#0d9488' }} /> host</span>
                <span className="flex items-center gap-1.5"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: '#cbd5e1' }} /> guest</span>
              </div>
            </div>

            <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
              <h2 className="text-base font-semibold text-gray-900 mb-1">People credited per episode</h2>
              <p className="text-sm text-gray-500 mb-4">
                The zero column is the backlog. A long tail is the other failure: a list of
                names in the show notes read as though everyone appeared.
              </p>
              <div className="flex items-end gap-1.5 h-40">
                {perEpisode.map(b => (
                  <div key={b.credits} className="flex-1 flex flex-col items-center justify-end group" title={`${b.episodes.toLocaleString()} episodes with ${b.credits}`}>
                    <span className="text-[10px] text-gray-400 mb-1 opacity-0 group-hover:opacity-100">
                      {b.episodes.toLocaleString()}
                    </span>
                    <div
                      className="w-full rounded-t-sm"
                      style={{
                        height: `${Math.max(2, (100 * b.episodes) / maxBucket)}%`,
                        background: b.credits === 0 ? '#ef4444' : teal(Math.min(1, b.credits / 6)),
                      }}
                    />
                    <span className="text-[10px] text-gray-500 mt-1">{b.credits}</span>
                  </div>
                ))}
              </div>
              <p className="text-xs text-gray-400 mt-3">
                {perEpisode[0]?.episodes.toLocaleString()} of {totalEpisodes.toLocaleString()} episodes
                ({pct(perEpisode[0]?.episodes || 0, totalEpisodes)}%) have no credits at all.
              </p>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
