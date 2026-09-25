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

function ShowLink({ show, children }) {
  return (
    <a href={`/admin/shows?apple_podcast_id=${show.apple_podcast_id}`}
       className="hover:text-teal-600 hover:underline">
      {children}
    </a>
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

const WEEK_FMT = new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric' });

// "12d ago" in the show tables, red once the show has missed about three of
// its usual releases (see _add_publishing_rhythm in the backend).
function LastEpisode({ show }) {
  if (show.days_since == null) return <span className="text-gray-300">—</span>;
  const cls = show.freshness === 'overdue' ? 'text-red-600 font-medium'
    : show.freshness === 'ended' ? 'text-gray-400' : 'text-gray-500';
  const tip = `Last episode ${show.last_episode}` +
    (show.typical_gap ? ` · usually every ${show.typical_gap} day${show.typical_gap === 1 ? '' : 's'}` : '') +
    (show.freshness === 'overdue' ? ' · overdue: check the scanner is still picking it up'
      : show.freshness === 'ended' ? ' · silent long enough that it has probably ended' : '');
  return (
    <span className={cls} title={tip}>
      {show.days_since}d{show.freshness === 'ended' ? ' · ended?' : ''}
      {show.missing_newer && <span className="ml-1 text-red-600" title={`Apple has an episode from ${show.apple_latest}`}>· behind</span>}
    </span>
  );
}

// What "coverage" means in the main table — which of these an episode needs
// at least one of to count as covered, and how to talk about it.
const COVERAGE_METRICS = {
  any:   { label: 'Any credit',   key: 'episodes_with_credit', colLabel: 'Episodes credited',    noun: 'a credit', creditFilter: 'no_credit' },
  host:  { label: 'Host credit',  key: 'episodes_with_host',   colLabel: 'Episodes with a host',  noun: 'a host',   creditFilter: 'no_host' },
  // denomKey: an episode confirmed to genuinely have no guest is excluded
  // from what "100%" means for this metric, not counted against the show —
  // see migrate_add_no_guest_confirmed.sql.
  guest: { label: 'Guest credit', key: 'episodes_with_guest',  colLabel: 'Episodes with a guest', noun: 'a guest',  creditFilter: 'no_guest', denomKey: 'episodes_guest_eligible' },
};

export default function AdminDiagnostics() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [sort, setSort] = useState({ field: 'coverage', dir: 'asc' });
  const [minEpisodes, setMinEpisodes] = useState(20);
  const [metric, setMetric] = useState('any');
  const [pipeline, setPipeline] = useState(null);
  const [pipelineError, setPipelineError] = useState(null);
  const [dataHealth, setDataHealth] = useState(null);
  const [dataError, setDataError] = useState(null);
  const [repairing, setRepairing] = useState(null);
  const [repairNote, setRepairNote] = useState('');
  // "Worth looking at" lists open in full on request; 6 each by default.
  const [allUncovered, setAllUncovered] = useState(false);
  const [allMissingHosts, setAllMissingHosts] = useState(false);

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

  // Slower (it works out everyone's current role), so it loads on its own
  // and the show tables don't wait for it.
  useEffect(() => {
    adminFetch(`${API}/diagnostics/pipeline`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setPipeline)
      .catch(e => setPipelineError(e.message || 'Failed to load'));
    adminFetch(`${API}/diagnostics/data`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setDataHealth)
      .catch(e => setDataError(e.message || 'Failed to load'));
  }, []);

  // Keeps every credit (unlike a rename in People Admin, which unlinks the
  // inferred ones and re-scans), then credits episodes that use the real spelling.
  const repairName = async (m) => {
    setRepairing(m.host_id); setRepairNote('');
    try {
      const r = await adminFetch(`${API}/people/${m.host_id}/repair-name`, { method: 'POST' });
      const body = await r.json();
      if (!r.ok) throw new Error(body.detail || `API error ${r.status}`);
      setRepairNote(`Fixed “${body.name}” — ${body.episodes_linked} more episode${body.episodes_linked === 1 ? '' : 's'} credited`);
      await load();
    } catch (e) {
      setRepairNote(`Couldn't fix ${m.repaired}: ${e.message}`);
    } finally {
      setRepairing(null);
    }
  };

  const shows = useMemo(() => {
    if (!data) return [];
    const coverageKey = COVERAGE_METRICS[metric].key;
    const denomKey = COVERAGE_METRICS[metric].denomKey || 'episodes';
    const rows = data.shows
      .filter(s => s.episodes >= minEpisodes)
      .map(s => ({
        ...s,
        coverage: pct(s[coverageKey], s[denomKey]),
        coverageCount: s[coverageKey],
        coverageDenom: s[denomKey],
        pctGuest: pct(s.guest_credits, s.credits),
        pctApple: pct(s.apple_credits, s.credits),
        // Sorts overdue shows by how many releases they've missed.
        lateness: s.days_since != null && s.typical_gap ? s.days_since / s.typical_gap : -1,
      }));
    const dir = sort.dir === 'asc' ? 1 : -1;
    return rows.sort((a, b) => {
      const av = a[sort.field], bv = b[sort.field];
      if (typeof av === 'string') return dir * av.localeCompare(bv);
      return dir * (av - bv);
    });
  }, [data, sort, minEpisodes, metric]);

  const perEpisode = data?.credits_per_episode || [];
  const mangled = data?.mangled_names || [];
  const maxBucket = Math.max(1, ...perEpisode.map(b => b.episodes));
  const totalEpisodes = perEpisode.reduce((n, b) => n + b.episodes, 0);

  // A show credited as all-guest with nobody registered as its host is not a
  // finding about the show; it means we never recorded who presents it.
  const missingHosts = shows.filter(s => s.credits >= 20 && s.pctGuest >= 85 && s.registered_hosts === 0);
  // Shows whose descriptions we deliberately don't read are expected to be
  // thin, so they belong outside the list of things to look into.
  const uncovered = shows.filter(s => s.coverage <= 20 && s.scan_descriptions !== false);
  // Apple lists a newer episode than we have: the scanner is behind.
  const behind = (data?.shows || []).filter(s => s.missing_newer)
    .sort((a, b) => (a.last_episode || '').localeCompare(b.last_episode || ''));
  const overdue = (data?.shows || []).filter(s => s.freshness === 'overdue' && !s.missing_newer)
    .sort((a, b) => b.days_since / b.typical_gap - a.days_since / a.typical_gap);

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
            {mangled.length > 0 && (
              <div className="bg-white rounded-2xl border border-amber-200 p-4 sm:p-5">
                <h2 className="text-base font-semibold text-gray-900 mb-1">
                  {mangled.length} name{mangled.length === 1 ? '' : 's'} with mangled characters
                </h2>
                <p className="text-sm text-gray-500 mb-3">
                  Stored with their UTF-8 bytes read as Latin-1, so "Balázs" becomes
                  "BalÃ¡zs". The scanner matches the real spelling in an episode
                  against the stored one, so these people can never gain a credit
                  and sit frozen at whatever they arrived with.
                </p>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-xs uppercase tracking-wide text-gray-400">
                      <th className="pb-1 font-medium">Stored as</th>
                      <th className="pb-1 font-medium">Should be</th>
                      <th className="pb-1 font-medium text-right">Credits</th>
                      <th className="pb-1" />
                    </tr>
                  </thead>
                  <tbody>
                    {mangled.map(m => (
                      <tr key={m.host_id} className="border-t border-gray-100">
                        <td className="py-1.5 font-mono text-xs text-red-600">{m.stored}</td>
                        <td className="py-1.5">
                          <a href={`/admin/people?host_id=${m.host_id}`}
                             className="font-medium hover:text-teal-600 hover:underline">
                            {m.repaired}
                          </a>
                        </td>
                        <td className="py-1.5 text-right text-gray-500">{m.credits}</td>
                        <td className="py-1.5 text-right">
                          <button onClick={() => repairName(m)} disabled={repairing !== null}
                            className="text-xs px-2 py-0.5 rounded border border-teal-600 text-teal-700 hover:bg-teal-50 disabled:opacity-50">
                            {repairing === m.host_id ? 'Fixing…' : 'Fix'}
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            {repairNote && <p className="text-sm text-gray-600">{repairNote}</p>}

            <ScannerHealth weeks={pipeline?.weeks} overdue={overdue} behind={behind} error={pipelineError} />

            {(uncovered.length > 0 || missingHosts.length > 0) && (
              <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-5">
                <h2 className="text-base font-semibold text-gray-900 mb-3">Worth looking at</h2>
                <div className="grid gap-4 sm:grid-cols-2 text-sm">
                  <div>
                    <p className="text-gray-500 mb-1">
                      {uncovered.length} show{uncovered.length === 1 ? '' : 's'} with 20% or less of episodes credited
                    </p>
                    <ul className="text-gray-800 space-y-0.5">
                      {(allUncovered ? uncovered : uncovered.slice(0, 6)).map(s => (
                        <li key={s.podcast_id}>
                          <ShowLink show={s}>{s.title}</ShowLink> <span className="text-gray-400">— {s.coverage}% of {s.episodes.toLocaleString()}</span>
                          <a href={`/admin/episodes?show=${encodeURIComponent(s.title)}&credit_filter=${COVERAGE_METRICS[metric].creditFilter}`}
                             title={`View episodes missing ${COVERAGE_METRICS[metric].noun}`}
                             className="ml-1.5 text-teal-600 hover:text-teal-800">→</a>
                        </li>
                      ))}
                    </ul>
                    {uncovered.length > 6 && (
                      <button onClick={() => setAllUncovered(v => !v)} className="mt-1 text-xs text-teal-700 hover:underline">
                        {allUncovered ? 'Show fewer' : `Show all ${uncovered.length}`}
                      </button>
                    )}
                  </div>
                  <div>
                    <p className="text-gray-500 mb-1">
                      {missingHosts.length} show{missingHosts.length === 1 ? '' : 's'} credited as nearly all guests, with no host registered
                    </p>
                    <ul className="text-gray-800 space-y-0.5">
                      {(allMissingHosts ? missingHosts : missingHosts.slice(0, 6)).map(s => (
                        <li key={s.podcast_id}>
                          <ShowLink show={s}>{s.title}</ShowLink> <span className="text-gray-400">— {s.pctGuest}% guest</span>
                        </li>
                      ))}
                    </ul>
                    {missingHosts.length > 6 && (
                      <button onClick={() => setAllMissingHosts(v => !v)} className="mt-1 text-xs text-teal-700 hover:underline">
                        {allMissingHosts ? 'Show fewer' : `Show all ${missingHosts.length}`}
                      </button>
                    )}
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
                      <SortHeader label="Last ep." field="lateness" sort={sort} setSort={setSort} />
                    </tr>
                  </thead>
                  <tbody>
                    {shows.map(s => (
                      <tr key={s.podcast_id} className="border-b border-gray-100 hover:bg-gray-50">
                        <td className="px-2 py-1.5 text-gray-900">
                          <ShowLink show={s}>{s.title}</ShowLink>
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
                               title={`${s.coverageCount} of ${s.coverageDenom} episodes have ${COVERAGE_METRICS[metric].noun}` +
                                 (s.coverageDenom !== s.episodes ? ` (${s.episodes - s.coverageDenom} confirmed to have no guest, excluded)` : '')} />
                        </td>
                        <td className={`px-2 py-1.5 text-right tabular-nums ${
                          s.coverage <= 20 && s.scan_descriptions !== false ? 'text-red-600 font-medium' : 'text-gray-600'}`}>
                          {s.coverage}%
                          {s.coverage < 100 && (
                            <a href={`/admin/episodes?show=${encodeURIComponent(s.title)}&credit_filter=${COVERAGE_METRICS[metric].creditFilter}`}
                               title={`View episodes missing ${COVERAGE_METRICS[metric].noun}`}
                               className="ml-1.5 text-teal-600 hover:text-teal-800 no-underline">→</a>
                          )}
                        </td>
                        <td className="px-2 py-1.5">
                          <Bar value={s.pctApple} max={100} color={teal(s.pctApple / 100)}
                               title={`${s.apple_credits} of ${s.credits} credits from Apple`} />
                        </td>
                        <td className="px-2 py-1.5 text-right text-gray-600 tabular-nums">{s.pctApple}%</td>
                        <td className="px-2 py-1.5 text-right text-gray-500 tabular-nums">{s.credits.toLocaleString()}</td>
                        <td className="px-2 py-1.5 text-right tabular-nums whitespace-nowrap"><LastEpisode show={s} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <RoleExtraction pipeline={pipeline} error={pipelineError} />
            <CompanyData data={dataHealth} error={dataError} />
            <PeopleData data={dataHealth} error={dataError} />

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
                        <td className="px-2 py-1.5 text-gray-900"><ShowLink show={s}>{s.title}</ShowLink></td>
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
                names in the show notes read as though everyone appeared. Click a bar to list
                those episodes.
              </p>
              <div className="flex items-end gap-1.5 h-40">
                {perEpisode.map(b => (
                  <a key={b.credits}
                     href={`/admin/episodes?credit_filter=${b.credits === 0 ? 'no_credit' : `count_${b.credits}`}`}
                     className="flex-1 h-full flex flex-col items-center justify-end group"
                     title={`${b.episodes.toLocaleString()} episodes with ${b.credits} — click to list them`}>
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
                    <span className="text-[10px] text-gray-500 mt-1 group-hover:text-teal-700">{b.credits}</span>
                  </a>
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


function Card({ title, children, description }) {
  return (
    <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
      <h2 className="text-base font-semibold text-gray-900 mb-1">{title}</h2>
      {description && <p className="text-sm text-gray-500 mb-4">{description}</p>}
      {children}
    </div>
  );
}

// Episodes by the week they were published, which should be roughly flat: a
// dip in recent weeks means the scanner is falling behind. The number under
// each bar is how many episodes were added to the database that week, which
// spikes on back-catalogue imports and is shown for context only.
function ScannerHealth({ weeks, overdue, behind = [], error }) {
  const [showAll, setShowAll] = useState(false);
  if (error) return <Card title="Scanner health"><p className="text-sm text-red-500">Couldn't load: {error}</p></Card>;
  if (!weeks) return <Card title="Scanner health"><p className="text-sm text-gray-400">Loading…</p></Card>;
  const full = weeks.slice(0, -1);   // the current week is still filling up
  const typical = [...full.map(w => w.published)].sort((a, b) => a - b)[Math.floor(full.length / 2)] || 0;
  const max = Math.max(1, ...weeks.map(w => w.published));
  const listed = showAll ? overdue : overdue.slice(0, 8);
  return (
    <Card title="Scanner health"
      description="New episodes by the week they were published. It should stay roughly level; recent weeks falling short means new episodes aren't being picked up.">
      <div className="flex items-end gap-1.5 h-32">
        {weeks.map((w, i) => {
          const current = i === weeks.length - 1;
          const low = !current && typical > 0 && w.published < 0.75 * typical;
          return (
            <div key={w.week} className="flex-1 h-full flex flex-col items-center justify-end group"
                 title={`Week of ${w.week}: ${w.published} published, ${w.added.toLocaleString()} added to the database${current ? ' (week in progress)' : ''}`}>
              <span className={`text-[10px] mb-1 ${low ? 'text-red-600 font-medium' : 'text-gray-400'}`}>{w.published}</span>
              <div className="w-full rounded-t-sm"
                   style={{ height: `${Math.max(2, (100 * w.published) / max)}%`,
                            background: low ? '#ef4444' : current ? '#99f6e4' : '#0d9488' }} />
              <span className="text-[10px] text-gray-500 mt-1 whitespace-nowrap">{WEEK_FMT.format(new Date(w.week + 'T00:00:00'))}</span>
              <span className="text-[9px] text-gray-300">+{w.added.toLocaleString()}</span>
            </div>
          );
        })}
      </div>
      <p className="text-xs text-gray-400 mt-2">
        Typical week: {typical} episodes. Red: under three-quarters of that. Palest bar: this week, still in progress.
      </p>

      <h3 className="text-sm font-semibold text-gray-900 mt-5 mb-1">
        {behind.length} show{behind.length === 1 ? '' : 's'} missing new episodes
      </h3>
      <p className="text-sm text-gray-500 mb-2">
        Apple lists a newer episode than we have (checked on every scrape run). Each scrape run should
        catch these up; any that stay here are a scanner problem.
      </p>
      {behind.length > 0 && (
        <ul className="text-sm space-y-0.5 mb-2">
          {behind.map(s => (
            <li key={s.podcast_id}>
              <ShowLink show={s}>{s.title}</ShowLink>
              <span className="text-gray-400"> — ours {s.last_episode}, Apple {s.apple_latest}</span>
            </li>
          ))}
        </ul>
      )}

      <h3 className="text-sm font-semibold text-gray-900 mt-5 mb-1">
        {overdue.length} show{overdue.length === 1 ? '' : 's'} quiet
      </h3>
      <p className="text-sm text-gray-500 mb-2">
        Silent for more than three of their usual gaps between episodes, and Apple has nothing newer either:
        the show has paused, not the scanner.
      </p>
      {overdue.length > 0 && (
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs uppercase tracking-wide text-gray-400">
              <th className="pb-1 font-medium">Show</th>
              <th className="pb-1 font-medium text-right">Last episode</th>
              <th className="pb-1 font-medium text-right">Usually every</th>
              <th className="pb-1 font-medium text-right">Missed</th>
            </tr>
          </thead>
          <tbody>
            {listed.map(s => (
              <tr key={s.podcast_id} className="border-t border-gray-100">
                <td className="py-1.5"><ShowLink show={s}>{s.title}</ShowLink></td>
                <td className="py-1.5 text-right text-gray-500 tabular-nums">{s.last_episode} ({s.days_since}d)</td>
                <td className="py-1.5 text-right text-gray-500 tabular-nums">{s.typical_gap}d</td>
                <td className="py-1.5 text-right text-red-600 font-medium tabular-nums">~{Math.floor(s.days_since / s.typical_gap)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {overdue.length > 8 && (
        <button onClick={() => setShowAll(v => !v)} className="mt-2 text-xs text-teal-700 hover:underline">
          {showAll ? 'Show fewer' : `Show all ${overdue.length}`}
        </button>
      )}
    </Card>
  );
}

const QUALITY = [
  { key: 'full',         label: 'Title and company', filter: 'role_title_company', color: '#0d9488' },
  { key: 'title_only',   label: 'Title only',        filter: 'role_title_only',    color: '#f59e0b' },
  { key: 'company_only', label: 'Company only',      filter: 'role_company_only',  color: '#fcd34d' },
  { key: 'none',         label: 'Nothing',           filter: 'role_none',          color: '#ef4444' },
];

const STATUS_LABELS = {
  done: 'Read', waiting: 'Waiting to be read', no_mention: 'Not named in the text',
  host: 'Turned out to be a host', retry: 'To retry', failed: 'Failed',
};

// The role pipeline: guest appearances waiting for extraction, what the
// extraction found per show, and how complete the role people actually see is.
function RoleExtraction({ pipeline, error }) {
  const [sort, setSort] = useState({ field: 'pctRole', dir: 'asc' });
  const [showAll, setShowAll] = useState(false);
  const rows = useMemo(() => {
    if (!pipeline) return [];
    const dir = sort.dir === 'asc' ? 1 : -1;
    return pipeline.role_by_show
      .map(r => ({ ...r, pctRole: r.done ? pct(r.with_role, r.done) : -1, pctWaiting: pct(r.waiting, r.appearances) }))
      .sort((a, b) => {
        // Shows not read yet have no share to rank, so they stay at the bottom.
        if (sort.field === 'pctRole' && (!a.done || !b.done)) return (!a.done) - (!b.done) || b.appearances - a.appearances;
        const av = a[sort.field], bv = b[sort.field];
        return typeof av === 'string' ? dir * av.localeCompare(bv) : dir * (av - bv) || b.appearances - a.appearances;
      });
  }, [pipeline, sort]);
  if (error) return <Card title="Roles"><p className="text-sm text-red-500">Couldn't load: {error}</p></Card>;
  if (!pipeline) return <Card title="Roles"><p className="text-sm text-gray-400">Loading…</p></Card>;

  const q = pipeline.role_quality;
  const qTotal = QUALITY.reduce((n, b) => n + q[b.key], 0);
  const statusTotal = pipeline.extraction.reduce((n, r) => n + r.n, 0);
  const waiting = pipeline.extraction.filter(r => r.status === 'waiting' || r.status === 'retry').reduce((n, r) => n + r.n, 0);
  const listed = showAll ? rows : rows.slice(0, 15);
  return (
    <Card title="Roles"
      description="What each guest does and where, read from the episode text, and how complete the role shown on their card is.">
      <div className="grid gap-6 sm:grid-cols-2">
        <div>
          <h3 className="text-sm font-semibold text-gray-900 mb-2">Current role shown for guests</h3>
          <div className="flex h-3 rounded-sm overflow-hidden bg-gray-100 mb-2">
            {QUALITY.map(b => (
              <a key={b.key} href={`/admin/people?filter=${b.filter}`} title={`${b.label}: ${q[b.key].toLocaleString()}`}
                 style={{ width: `${(100 * q[b.key]) / Math.max(1, qTotal)}%`, background: b.color }} />
            ))}
          </div>
          <ul className="text-sm space-y-0.5">
            {QUALITY.map(b => (
              <li key={b.key} className="flex items-center gap-2">
                <span className="w-3 h-2 rounded-sm inline-block" style={{ background: b.color }} />
                <a href={`/admin/people?filter=${b.filter}`} className="text-gray-700 hover:text-teal-700 hover:underline">{b.label}</a>
                <span className="ml-auto tabular-nums text-gray-500">{q[b.key].toLocaleString()} · {pct(q[b.key], qTotal)}%</span>
              </li>
            ))}
          </ul>
          <p className="text-xs text-gray-400 mt-2">
            {qTotal.toLocaleString()} people credited as a guest at least once. Each line opens People Admin on
            that filter (which also counts hosts).
          </p>
        </div>
        <div>
          <h3 className="text-sm font-semibold text-gray-900 mb-2">Guest appearances</h3>
          <ul className="text-sm space-y-0.5">
            {pipeline.extraction.map(r => (
              <li key={r.status} className="flex">
                <span className={r.status === 'waiting' || r.status === 'retry' ? 'text-amber-700' : 'text-gray-700'}>
                  {STATUS_LABELS[r.status] || r.status}
                </span>
                <span className="ml-auto tabular-nums text-gray-500">{r.n.toLocaleString()} · {pct(r.n, statusTotal)}%</span>
              </li>
            ))}
          </ul>
          <p className="text-xs text-gray-400 mt-2">
            {waiting.toLocaleString()} waiting for the next extraction run.{' '}
            {pipeline.guests_without_role.toLocaleString()} guests have no role on record from any appearance.
          </p>
        </div>
      </div>

      <h3 className="text-sm font-semibold text-gray-900 mt-6 mb-1">By show</h3>
      <p className="text-sm text-gray-500 mb-2">
        Of the guest appearances already read, how many gave a role. A low share means the show's notes rarely
        say what guests do, or the text sent for reading misses it. Shows with 10+ guest appearances.
      </p>
      <div className="overflow-x-auto">
        <table className="w-full text-sm border-collapse">
          <thead>
            <tr className="border-b border-gray-200 text-xs">
              <SortHeader label="Show" field="title" sort={sort} setSort={setSort} align="left" />
              <SortHeader label="Guest appearances" field="appearances" sort={sort} setSort={setSort} />
              <SortHeader label="Waiting" field="pctWaiting" sort={sort} setSort={setSort} />
              <th className="px-2 py-2 text-left font-medium text-gray-400 w-40">Read with a role</th>
              <SortHeader label="%" field="pctRole" sort={sort} setSort={setSort} />
            </tr>
          </thead>
          <tbody>
            {listed.map(r => (
              <tr key={r.podcast_id} className="border-b border-gray-100 hover:bg-gray-50">
                <td className="px-2 py-1.5 text-gray-900"><ShowLink show={r}>{r.title}</ShowLink></td>
                <td className="px-2 py-1.5 text-right text-gray-500 tabular-nums">{r.appearances.toLocaleString()}</td>
                <td className={`px-2 py-1.5 text-right tabular-nums ${r.waiting ? 'text-amber-700' : 'text-gray-300'}`}>
                  {r.waiting ? r.waiting.toLocaleString() : '—'}
                </td>
                <td className="px-2 py-1.5">
                  {r.done > 0
                    ? <Bar value={r.pctRole} max={100} color={teal(r.pctRole / 100)} title={`${r.with_role} of ${r.done} read appearances gave a role`} />
                    : <span className="text-xs text-gray-400">not read yet</span>}
                </td>
                <td className={`px-2 py-1.5 text-right tabular-nums ${r.done && r.pctRole <= 30 ? 'text-red-600 font-medium' : 'text-gray-600'}`}>
                  {r.done ? `${r.pctRole}%` : ''}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {rows.length > 15 && (
        <button onClick={() => setShowAll(v => !v)} className="mt-2 text-xs text-teal-700 hover:underline">
          {showAll ? 'Show fewer' : `Show all ${rows.length}`}
        </button>
      )}
    </Card>
  );
}

// One completeness row: a bar of how many have it, and a link to the ones
// that don't.
function HaveRow({ label, have, of, href, missingLabel }) {
  const p = pct(have, of);
  return (
    <li className="grid grid-cols-[9rem_1fr_7rem_8rem] items-center gap-3 py-1">
      <span className="text-gray-700">{label}</span>
      <Bar value={p} max={100} color={teal(p / 100)} title={`${have.toLocaleString()} of ${of.toLocaleString()}`} />
      <span className="text-right tabular-nums text-gray-500">{have.toLocaleString()} · {p}%</span>
      <span className="text-right text-xs">
        {href
          ? <a href={href} className="text-teal-700 hover:underline">{(of - have).toLocaleString()} {missingLabel || 'missing'} →</a>
          : <span className="text-gray-400">{(of - have).toLocaleString()} {missingLabel || 'missing'}</span>}
      </span>
    </li>
  );
}

function CompanyData({ data, error }) {
  const [busy, setBusy] = useState(true);
  if (error) return <Card title="Company data"><p className="text-sm text-red-500">Couldn't load: {error}</p></Card>;
  if (!data) return <Card title="Company data"><p className="text-sm text-gray-400">Loading…</p></Card>;
  const o = busy ? data.orgs.busy : data.orgs.all;
  return (
    <Card title="Company data"
      description="How complete the organisation records are. A gap matters most where several people work, so that's the default view.">
      <div className="flex text-xs border border-gray-300 rounded overflow-hidden w-fit mb-3">
        {[[true, `3+ people (${data.orgs.busy.orgs.toLocaleString()})`], [false, `All (${data.orgs.all.orgs.toLocaleString()})`]].map(([v, label]) => (
          <button key={label} onClick={() => setBusy(v)}
            className={`px-2 py-1 ${busy === v ? 'bg-teal-600 text-white' : 'bg-white text-gray-500 hover:bg-gray-50'}`}>
            {label}
          </button>
        ))}
      </div>
      <ul className="text-sm">
        <HaveRow label="Type" have={o.typed} of={o.orgs} href="/admin/companies?view=untyped" />
        <HaveRow label="Website (and logo)" have={o.website} of={o.orgs} href="/admin/companies?view=no_website" />
        <HaveRow label="Wikidata match" have={o.wikidata} of={o.orgs} />
        <HaveRow label="Parent organisation" have={o.parent} of={o.orgs} missingLabel="without" />
      </ul>
      <p className="text-xs text-gray-400 mt-2">
        Company Admin's lists cover all organisations, most people first. Most organisations have no parent,
        so that row is for reference, not a target.
      </p>
      <div className="flex flex-wrap gap-x-6 gap-y-1 mt-4 text-sm">
        <a href="/admin/companies/suggestions" className="text-teal-700 hover:underline">
          {data.merge_queue.toLocaleString()} open merge suggestions →
        </a>
        <a href="/admin/companies?view=not_org" className="text-gray-500 hover:underline">
          {data.not_orgs.toLocaleString()} marked not an organisation
        </a>
      </div>
    </Card>
  );
}

function PeopleData({ data, error }) {
  if (error) return <Card title="People data"><p className="text-sm text-red-500">Couldn't load: {error}</p></Card>;
  if (!data) return <Card title="People data"><p className="text-sm text-gray-400">Loading…</p></Card>;
  const p = data.people;
  const odd = data.org_like_names;
  return (
    <Card title="People data"
      description={`Profile links for the ${p.people.toLocaleString()} people credited as a guest at least once, from show notes and Wikidata or typed in People Admin.`}>
      <ul className="text-sm">
        <HaveRow label="LinkedIn" have={p.linkedin} of={p.people} href="/admin/people?filter=no_linkedin" />
        <HaveRow label="Photo" have={p.photo} of={p.people} href="/admin/people?filter=no_image" />
        <HaveRow label="X / Twitter" have={p.twitter} of={p.people} />
        <HaveRow label="Bluesky" have={p.bluesky} of={p.people} />
        <HaveRow label="Wikipedia" have={p.wikipedia} of={p.people} />
        <HaveRow label="Wikidata match" have={p.wikidata} of={p.people} />
      </ul>
      <p className="text-xs text-gray-400 mt-2">People Admin's filters cover everyone, hosts included, most appearances first.</p>

      <h3 className="text-sm font-semibold text-gray-900 mt-5 mb-1">
        {odd.length} name{odd.length === 1 ? '' : 's'} that look like an organisation or show
      </h3>
      <p className="text-sm text-gray-500 mb-2">
        A credit line read as a person ("Planet Money"). Not proof: a real name can trip it, so check before deleting.
      </p>
      {odd.length > 0 && (
        <ul className="text-sm space-y-0.5">
          {odd.map(m => (
            <li key={m.host_id}>
              <a href={`/admin/people?host_id=${m.host_id}`} className="text-gray-800 hover:text-teal-700 hover:underline">{m.name}</a>
              <span className="text-gray-400"> — {m.credits} credit{m.credits === 1 ? '' : 's'}</span>
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}
