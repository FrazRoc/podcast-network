import { useState, useEffect, useCallback, useRef } from 'react';
import { API_BASE_URL } from '../config';
import { adminFetch } from '../adminAuth';
import AdminHeader from './AdminHeader';
import AdminListCount from './AdminListCount';
import AdminSubTabs from './AdminSubTabs';
import TopicSuggestions, { REASONS, mergeTopics, markDifferent } from './AdminTopicSuggestions';
import { formatDateOnly } from '../adminUtils';
import { Swatch, CATEGORIES } from './Topics';
import { topicHref, orgHref, personHref, slugify } from '../profileUtils';
import { CompanyPicker } from './AdminCompanies';

// Topic Admin: rename topics, change their category, mark ones that aren't
// really topics, and merge duplicates ("geothermal" into "geothermal
// energy"). A merge moves the episodes and every spelling, so the tagger
// files future mentions under the survivor. A narrower subject isn't a
// duplicate: set the topic it sits under instead ("home batteries" under
// "Energy storage and batteries"); broad topics are the level under the
// twelve categories. Company and person tags ("Tesla",
// "Joe Manchin") aren't topics either: flag them and link the company or
// person, and they move to that page.

const API = `${API_BASE_URL}/api/admin/topics`;

const VIEWS = [{ id: 'active', label: 'Topics' }, { id: 'broad', label: 'Broad topics' }, { id: 'company', label: 'Companies' },
  { id: 'person', label: 'People' }, { id: 'not_topic', label: 'Not a topic' }];

// Search-as-you-type over people, for linking a person tag to a profile.
function PersonPicker({ onPick }) {
  const [q, setQ] = useState('');
  const [results, setResults] = useState([]);
  useEffect(() => {
    if (q.trim().length < 2) { setResults([]); return undefined; }
    const t = setTimeout(() => {
      adminFetch(`${API_BASE_URL}/api/admin/people?q=${encodeURIComponent(q.trim())}&limit=8`)
        .then(r => (r.ok ? r.json() : Promise.reject(r)))
        .then(d => setResults(d.items || []))
        .catch(() => setResults([]));
    }, 250);
    return () => clearTimeout(t);
  }, [q]);
  return (
    <div className="relative">
      <input type="text" value={q} onChange={e => setQ(e.target.value)} placeholder="Link to a person in the directory…"
        className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
      {results.length > 0 && (
        <div className="absolute z-10 mt-1 w-full bg-white border border-gray-200 rounded-lg shadow-lg max-h-56 overflow-y-auto">
          {results.map(h => (
            <button key={h.host_id} onClick={() => { onPick(h); setQ(''); setResults([]); }}
              className="w-full text-left px-3 py-2 text-sm hover:bg-gray-50 flex items-center justify-between gap-2">
              <span className="truncate">{h.first_name} {h.last_name}</span>
              <span className="text-xs text-gray-400 flex-shrink-0">{h.appearances} episodes</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
const SORTS = [
  { id: 'episodes_desc', label: 'Most episodes' },
  { id: 'name_asc', label: 'Name A–Z' },
  { id: 'newest', label: 'Newest' },
];

const readTagId = () => {
  try { return parseInt(new URLSearchParams(window.location.search).get('tag_id'), 10) || null; } catch { return null; }
};

function MergeBox({ topic, onMerged }) {
  const [q, setQ] = useState('');
  const [hits, setHits] = useState([]);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState('');
  useEffect(() => {
    if (q.trim().length < 2) { setHits([]); return undefined; }
    const t = setTimeout(() => {
      adminFetch(`${API}?q=${encodeURIComponent(q.trim())}&limit=12`).then(r => r.json())
        .then(d => setHits((d.items || []).filter(i => i.tag_id !== topic.tag_id))).catch(() => setHits([]));
    }, 250);
    return () => clearTimeout(t);
  }, [q, topic.tag_id]);

  const merge = async (other) => {
    if (!window.confirm(`Merge "${other.name}" (${other.episodes} episodes) into "${topic.name}"?\n\n`
      + `"${other.name}" will be deleted; its episodes and spellings move to "${topic.name}".`)) return;
    setBusy(true);
    const r = await adminFetch(`${API}/${topic.tag_id}/merge/${other.tag_id}`, { method: 'POST' });
    const d = await r.json().catch(() => ({}));
    setBusy(false);
    if (r.ok) {
      setMsg(`Merged "${d.merged}": ${d.episodes_moved} episode rows, ${d.aliases_moved} spellings.`);
      setQ(''); setHits([]);
      onMerged();
    } else setMsg(d.detail || 'Merge failed');
  };

  return (
    <div>
      <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1">Merge a duplicate into this topic</p>
      <input value={q} onChange={e => setQ(e.target.value)} placeholder="Search for the duplicate…"
        className="w-full rounded-lg border border-gray-300 px-3 py-1.5 text-sm focus:border-blue-500 focus:outline-none" />
      {hits.length > 0 && (
        <ul className="mt-1 border border-gray-200 rounded-lg divide-y divide-gray-100 max-h-56 overflow-y-auto">
          {hits.map(h => (
            <li key={h.tag_id} className="px-3 py-1.5 flex items-center gap-2 text-sm">
              <Swatch category={h.category} />
              <span className="flex-1 truncate">{h.name}{h.not_a_topic && <span className="text-gray-400"> (not a topic)</span>}</span>
              <span className="text-xs text-gray-400">{h.episodes}</span>
              <button disabled={busy} onClick={() => merge(h)}
                className="text-xs px-2 py-0.5 rounded bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Merge in</button>
            </li>
          ))}
        </ul>
      )}
      {msg && <p className="mt-1 text-xs text-gray-600">{msg}</p>}
    </div>
  );
}

// Where a topic sits: its parent (a broad topic, or a broader topic), set by
// searching. The backend refuses a parent that would loop.
function ParentBox({ topic, saving, onSave }) {
  const [q, setQ] = useState('');
  const [hits, setHits] = useState([]);
  useEffect(() => {
    if (q.trim().length < 2) { setHits([]); return undefined; }
    const t = setTimeout(() => {
      adminFetch(`${API}?q=${encodeURIComponent(q.trim())}&limit=12`).then(r => r.json())
        .then(d => setHits((d.items || []).filter(i => i.tag_id !== topic.tag_id))).catch(() => setHits([]));
    }, 250);
    return () => clearTimeout(t);
  }, [q, topic.tag_id]);
  return (
    <div>
      <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1">Sits under</p>
      {topic.parent_tag_id ? (
        <p className="text-sm text-gray-700 mb-1">
          <a href={`/admin/topics?tag_id=${topic.parent_tag_id}`} className="text-blue-700 hover:underline">{topic.parent_name}</a>
          <button onClick={() => onSave({ parent_tag_id: 0 })} disabled={saving}
            className="ml-2 text-xs text-gray-400 hover:text-red-600">remove</button>
        </p>
      ) : <p className="text-sm text-gray-400 mb-1">Nothing — a top-level topic</p>}
      <input value={q} onChange={e => setQ(e.target.value)} placeholder="Move it under another topic…"
        className="w-full rounded-lg border border-gray-300 px-3 py-1.5 text-sm focus:border-blue-500 focus:outline-none" />
      {hits.length > 0 && (
        <ul className="mt-1 border border-gray-200 rounded-lg divide-y divide-gray-100 max-h-56 overflow-y-auto">
          {hits.map(h => (
            <li key={h.tag_id} className="px-3 py-1.5 flex items-center gap-2 text-sm">
              <Swatch category={h.category} />
              <span className="flex-1 truncate">{h.name}{h.is_broad && <span className="text-gray-400"> (broad)</span>}</span>
              <span className="text-xs text-gray-400">{h.episodes}</span>
              <button disabled={saving} onClick={() => { onSave({ parent_tag_id: h.tag_id }); setQ(''); setHits([]); }}
                className="text-xs px-2 py-0.5 rounded bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Put under</button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

// Open merge suggestions naming this topic, decided from the panel.
function PossibleDuplicates({ topic, onDone }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const act = async (fn) => {
    setBusy(true); setError('');
    try { await fn(); onDone(); } catch (e) { setError(e.message); }
    setBusy(false);
  };
  const btn = 'text-xs px-2 py-0.5 rounded border border-gray-300 text-gray-700 hover:bg-gray-50 disabled:opacity-50';
  return (
    <div>
      <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1">Possible duplicates</p>
      <ul className="space-y-1.5">
        {topic.suggestions.map(o => (
          <li key={o.tag_id} className="text-sm">
            <div className="flex items-center gap-2">
              <a href={`/admin/topics?tag_id=${o.tag_id}`} className="flex-1 truncate text-blue-700 hover:underline">{o.name}</a>
              <span className="text-xs text-gray-400">{o.episodes}</span>
              <span className={`text-[10px] px-1 py-0.5 rounded ${(REASONS[o.reason] || REASONS.spelling).bg}`}>
                {(REASONS[o.reason] || REASONS.spelling).label}</span>
            </div>
            <div className="mt-0.5 flex flex-wrap gap-1">
              <button disabled={busy} className={btn} onClick={() => act(() => mergeTopics(topic.tag_id, o.tag_id))}>Merge it into this</button>
              <button disabled={busy} className={btn} onClick={() => act(() => mergeTopics(o.tag_id, topic.tag_id))}>Merge this into it</button>
              <button disabled={busy} className={btn} onClick={() => act(() => markDifferent(topic.tag_id, o.tag_id))}>Different</button>
            </div>
          </li>
        ))}
      </ul>
      {error && <p className="mt-1 text-xs text-red-500">{error}</p>}
    </div>
  );
}

function TopicPanel({ tagId, onChanged, onClose }) {
  const [t, setT] = useState(null);
  const [name, setName] = useState('');
  const [error, setError] = useState('');
  const [saving, setSaving] = useState(false);
  const req = useRef(0);

  const load = useCallback(() => {
    const id = ++req.current;
    adminFetch(`${API}/${tagId}`).then(r => (r.ok ? r.json() : Promise.reject(r)))
      .then(d => { if (id === req.current) { setT(d); setName(d.name); setError(''); } })
      .catch(() => { if (id === req.current) setError('Couldn\'t load this topic'); });
  }, [tagId]);
  useEffect(() => { setT(null); load(); }, [load]);

  const save = async (patch) => {
    setSaving(true);
    const r = await adminFetch(`${API}/${tagId}`, {
      method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(patch) });
    const d = await r.json().catch(() => ({}));
    setSaving(false);
    if (!r.ok) { setError(d.detail || 'Save failed'); return; }
    setError('');
    load();
    onChanged();
  };

  if (error && !t) return <div className="bg-white rounded-2xl border border-gray-200 p-6 text-sm text-red-500">{error}</div>;
  if (!t) return <div className="bg-white rounded-2xl border border-gray-200 p-6 text-sm text-gray-400">Loading…</div>;
  return (
    <div className="bg-white rounded-2xl border border-gray-200 p-4 space-y-4">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="text-xs text-gray-400">Topic #{t.tag_id} · added {formatDateOnly(String(t.created_at).slice(0, 10))}</p>
          {t.is_company || t.is_person || t.not_a_topic ? <p className="text-lg font-semibold text-gray-900">{t.name}</p> : (
            <a href={topicHref(t.tag_id, slugify(t.name))} target="_blank" rel="noopener noreferrer"
              className="text-lg font-semibold text-gray-900 hover:underline">{t.name} ↗</a>
          )}
        </div>
        <button onClick={onClose} className="text-gray-400 hover:text-gray-600 text-sm">✕</button>
      </div>

      <div className="flex gap-2">
        <input value={name} onChange={e => setName(e.target.value)}
          className="flex-1 rounded-lg border border-gray-300 px-3 py-1.5 text-sm focus:border-blue-500 focus:outline-none" />
        <button disabled={saving || !name.trim() || name.trim() === t.name} onClick={() => save({ name })}
          className="px-3 py-1.5 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700 disabled:opacity-40">Rename</button>
      </div>
      <div className="flex flex-wrap items-center gap-3 text-sm">
        <label className="flex items-center gap-1.5 text-gray-500">Category
          <select value={t.category || ''} onChange={e => save({ category: e.target.value })} disabled={saving}
            className="border border-gray-300 rounded px-2 py-1 text-gray-800">
            {!t.category && <option value="">—</option>}
            {CATEGORIES.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </label>
        <label className="flex items-center gap-1.5 text-gray-500">
          <input type="checkbox" checked={t.not_a_topic} disabled={saving}
            onChange={e => save({ not_a_topic: e.target.checked })} />
          Not a topic (hide everywhere)
        </label>
        <label className="flex items-center gap-1.5 text-gray-500">
          <input type="checkbox" checked={t.is_broad} disabled={saving}
            onChange={e => save({ is_broad: e.target.checked })} />
          Broad topic
        </label>
      </div>
      {!t.is_company && !t.is_person && !t.not_a_topic && (
        <ParentBox topic={t} saving={saving} onSave={save} />
      )}
      {t.suggestions?.length > 0 && (
        <PossibleDuplicates topic={t} onDone={() => { load(); onChanged(); }} />
      )}
      {t.children?.length > 0 && (
        <div>
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1">
            Under it ({t.children.length}{t.children.length === 200 ? '+' : ''})
          </p>
          <div className="flex flex-wrap gap-1 max-h-32 overflow-y-auto">
            {t.children.map(c => (
              <a key={c.tag_id} href={`/admin/topics?tag_id=${c.tag_id}`}
                className="text-xs px-2 py-0.5 rounded bg-gray-100 text-gray-700 hover:bg-gray-200">{c.name} {c.episodes}</a>
            ))}
          </div>
        </div>
      )}
      {/* Companies aren't topics: a company tag leaves the topic pages and
          is listed on its organisation's page instead. */}
      <div className="space-y-2 text-sm">
        <label className="flex items-center gap-1.5 text-gray-500">
          <input type="checkbox" checked={t.is_company} disabled={saving}
            onChange={e => save({ is_company: e.target.checked })} />
          A company, investor or nonprofit, not a topic
        </label>
        {t.is_company && (
          t.org_id ? (
            <p className="text-gray-600">
              Listed on <a href={orgHref(t.org_id, slugify(t.org_name))} target="_blank" rel="noopener noreferrer"
                className="text-teal-700 hover:underline">{t.org_name} ↗</a>
              <button onClick={() => save({ org_id: 0 })} disabled={saving}
                className="ml-2 text-xs text-gray-400 hover:text-red-600">unlink</button>
            </p>
          ) : (
            <CompanyPicker placeholder="Link to a company in the directory…" onPick={o => save({ org_id: o.org_id })} />
          )
        )}
        <label className="flex items-center gap-1.5 text-gray-500">
          <input type="checkbox" checked={t.is_person} disabled={saving}
            onChange={e => save({ is_person: e.target.checked })} />
          A person, not a topic
        </label>
        {t.is_person && (
          t.host_id ? (
            <p className="text-gray-600">
              Listed on <a href={personHref(t.host_id, slugify(t.host_name))} target="_blank" rel="noopener noreferrer"
                className="text-teal-700 hover:underline">{t.host_name} ↗</a>
              <button onClick={() => save({ host_id: 0 })} disabled={saving}
                className="ml-2 text-xs text-gray-400 hover:text-red-600">unlink</button>
            </p>
          ) : (
            <PersonPicker onPick={h => save({ host_id: h.host_id })} />
          )
        )}
      </div>
      {error && <p className="text-sm text-red-600">{error}</p>}

      <div>
        <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1">Spellings</p>
        <div className="flex flex-wrap gap-1">
          {t.aliases.map(a => <span key={a.alias_id} className="text-xs px-2 py-0.5 rounded bg-gray-100 text-gray-700">{a.alias_name}</span>)}
        </div>
      </div>

      <MergeBox topic={{ tag_id: t.tag_id, name: t.name }} onMerged={() => { load(); onChanged(); }} />

      <div>
        <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1">Episodes ({t.episodes.length})</p>
        <ul className="divide-y divide-gray-100 max-h-[40vh] overflow-y-auto">
          {t.episodes.map(e => (
            <li key={e.episode_id} className="py-2 text-sm">
              <a href={`/admin/episodes?episode_id=${e.episode_id}`} className="text-gray-900 hover:underline">{e.title}</a>
              {e.is_primary && <span className="ml-1.5 text-[10px] px-1 py-0.5 rounded bg-teal-100 text-teal-800">main</span>}
              <p className="text-xs text-gray-500">{e.podcast_title} · {formatDateOnly(e.published_date)} · {e.data_source}</p>
              {e.evidence && <p className="text-xs text-gray-600 italic">“{e.evidence}”</p>}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

// Each tab has its own address, so a refresh or a shared link lands on it.
const TAB_PATHS = { topics: '/admin/topics', suggestions: '/admin/topics/suggestions' };
const tabFromPath = () => (window.location.pathname.startsWith(TAB_PATHS.suggestions) ? 'suggestions' : 'topics');

export default function AdminTopics() {
  const [tab, setTabState] = useState(tabFromPath);
  const setTab = (t) => {
    setTabState(t);
    if (window.location.pathname !== TAB_PATHS[t]) window.history.pushState(null, '', TAB_PATHS[t]);
  };
  useEffect(() => {
    const onPop = () => setTabState(tabFromPath());
    window.addEventListener('popstate', onPop);
    return () => window.removeEventListener('popstate', onPop);
  }, []);
  const [q, setQ] = useState('');
  const [view, setView] = useState('active');
  const [category, setCategory] = useState('');
  const [sort, setSort] = useState('episodes_desc');
  const [items, setItems] = useState([]);
  const [count, setCount] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [selected, setSelectedState] = useState(readTagId);
  const req = useRef(0);

  const setSelected = (id) => {
    setSelectedState(id);
    try { window.history.replaceState(null, '', id ? `/admin/topics?tag_id=${id}` : '/admin/topics'); } catch { /* sandboxed */ }
  };

  const load = useCallback(() => {
    const id = ++req.current;
    setLoading(true);
    const params = new URLSearchParams({ q, view, category, sort, limit: 300 });
    adminFetch(`${API}?${params}`).then(r => (r.ok ? r.json() : Promise.reject(r)))
      .then(d => {
        if (id !== req.current) return;
        setItems(d.items); setLoading(false); setError('');
        setCount({ total: d.total, allTotal: d.totals[view] ?? d.totals.active });
      })
      .catch(() => { if (id === req.current) { setLoading(false); setError('Couldn\'t load topics'); } });
  }, [q, view, category, sort]);
  useEffect(() => {
    const t = setTimeout(load, 200);
    return () => clearTimeout(t);
  }, [load]);

  return (
    <div className="min-h-screen bg-gray-100 text-left">
      <AdminHeader active="Topics" />
      <div className="p-4 sm:p-6">
        <AdminSubTabs active={tab} tabs={[
          { id: 'topics', label: 'Topics', onClick: () => setTab('topics') },
          { id: 'suggestions', label: 'Merge suggestions', onClick: () => setTab('suggestions') },
        ]} />
        {tab === 'suggestions' ? (
          <div className="max-w-3xl"><TopicSuggestions onChanged={load} /></div>
        ) : (
        <div className="flex flex-col md:flex-row gap-6">
          <div className="flex-1 min-w-0">
            <div className="bg-white rounded-2xl border border-gray-200 p-4 mb-3 space-y-3">
              <input type="text" value={q} onChange={e => setQ(e.target.value)} placeholder="Search topics or spellings…"
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
              <div className="flex gap-2 flex-wrap">
                {VIEWS.map(v => (
                  <button key={v.id} onClick={() => setView(v.id)}
                    className={`px-3 py-1 rounded-full text-xs font-medium ${view === v.id
                      ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}>
                    {v.label}
                  </button>
                ))}
              </div>
              <div className="flex items-center gap-3 flex-wrap text-xs">
                <label className="flex items-center gap-1 text-gray-400">Category:
                  <select value={category} onChange={e => setCategory(e.target.value)}
                    className="border border-gray-200 rounded px-2 py-1 text-gray-700">
                    <option value="">Any</option>
                    {CATEGORIES.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                </label>
                <label className="flex items-center gap-1 text-gray-400">Sort:
                  <select value={sort} onChange={e => setSort(e.target.value)}
                    className="border border-gray-200 rounded px-2 py-1 text-gray-700">
                    {SORTS.map(s => <option key={s.id} value={s.id}>{s.label}</option>)}
                  </select>
                </label>
              </div>
            </div>

            {count && <AdminListCount total={count.total} allTotal={count.allTotal} shown={items.length} noun="topic" />}
            <div className="bg-white rounded-2xl border border-gray-200 overflow-hidden overflow-y-auto max-h-[60vh] md:max-h-[calc(100vh-300px)]">
              {loading && items.length === 0 ? <div className="py-12 text-center text-gray-400 text-sm">Loading…</div>
                : error ? <div className="py-12 text-center text-red-500 text-sm">{error}</div>
                : items.length === 0 ? <div className="py-12 text-center text-gray-400 text-sm">No topics found</div>
                : (
                  <div className="divide-y divide-gray-100">
                    {items.map(t => (
                      <div key={t.tag_id} onClick={() => setSelected(t.tag_id === selected ? null : t.tag_id)}
                        className={`px-4 py-2.5 cursor-pointer hover:bg-gray-50 ${t.tag_id === selected ? 'bg-blue-50 border-l-2 border-blue-500' : ''}`}>
                        <div className="flex items-center gap-2">
                          <Swatch category={t.category} />
                          <p className="text-sm font-medium text-gray-900 truncate flex-1">{t.name}</p>
                          <span className="text-xs text-gray-400 flex-shrink-0">{t.episodes}</span>
                        </div>
                        <p className="text-xs text-gray-400 pl-4">
                          {t.category || 'No category'}{t.alias_count > 1 && ` · ${t.alias_count} spellings`}
                          {t.is_broad && ' · broad'}{t.parent_name && ` · under ${t.parent_name}`}
                          {view === 'company' && (t.org_name ? ` · → ${t.org_name}` : ' · not linked')}
                          {view === 'person' && (t.host_name ? ` · → ${t.host_name}` : ' · not linked')}
                        </p>
                      </div>
                    ))}
                  </div>
                )}
            </div>
          </div>

          <div className="w-full md:w-[30rem] flex-shrink-0">
            {selected ? (
              <TopicPanel key={selected} tagId={selected} onChanged={load} onClose={() => setSelected(null)} />
            ) : (
              <div className="bg-white rounded-2xl border border-gray-200 p-6 text-sm text-gray-400">
                Pick a topic to rename it, change its category, mark it not a topic, set the topic it sits under, or merge a duplicate into it.
              </div>
            )}
          </div>
        </div>
        )}
      </div>
    </div>
  );
}
