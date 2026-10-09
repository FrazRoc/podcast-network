import { useState, useEffect, useCallback } from 'react';
import { API_BASE_URL } from '../config';
import { adminFetch } from '../adminAuth';
import AdminListCount from './AdminListCount';
import { Swatch } from './Topics';

// Topic Admin's merge-suggestion queue: pairs of topics that are probably
// one topic (backend/topic_suggestions.py). Stored server-side and rebuilt
// after each tagging import or by Recompute, so this only reads a page.
// A narrower topic isn't a duplicate: "is under" sets the parent instead.

const API = `${API_BASE_URL}/api/admin`;

export const REASONS = {
  same_words: { label: 'Same words', bg: 'bg-blue-50 text-blue-700' },
  spelling: { label: 'Spelling variant', bg: 'bg-amber-50 text-amber-700' },
  acronym: { label: 'Acronym', bg: 'bg-purple-50 text-purple-700' },
};

export async function call(url, options) {
  const res = await adminFetch(url, options);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail || `API error ${res.status}`);
  return data;
}

const jsonBody = (method, body) => ({
  method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
});

export const mergeTopics = (keepId, dropId) => call(`${API}/topics/${keepId}/merge/${dropId}`, { method: 'POST' });
export const putUnder = (childId, parentId) => call(`${API}/topics/${childId}`, jsonBody('PUT', { parent_tag_id: parentId }));
export const markDifferent = (a, b) => call(`${API}/topics/not-same`, jsonBody('POST', { tag_a: a, tag_b: b }));

function Card({ s, onAction, onSkip }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const reason = REASONS[s.reason] || REASONS.spelling;

  // droppedId: a topic merged away, so every other card naming it is stale.
  const act = async (fn, droppedId = null) => {
    setBusy(true); setError('');
    try {
      await fn();
      onAction(s, droppedId);
    } catch (e) {
      if (/not found/i.test(e.message)) { onAction(s, null, true); return; }
      setError(e.message); setBusy(false);
    }
  };
  const merge = (keep, drop) => {
    if (!window.confirm(`Merge "${drop.name}" (${drop.episodes} episodes) into "${keep.name}"?`)) return;
    act(() => mergeTopics(keep.tag_id, drop.tag_id), drop.tag_id);
  };

  const side = (t) => (
    <div className="min-w-0">
      <a href={`/admin/topics?tag_id=${t.tag_id}`} target="_blank" rel="noopener noreferrer"
        className="flex items-center gap-1.5 text-sm font-medium text-gray-900 hover:underline min-w-0">
        <Swatch category={t.category} /><span className="truncate" title={t.name}>{t.name}</span>
      </a>
      <p className="text-xs text-gray-400">{t.episodes} episodes{t.parent_name ? ` · under ${t.parent_name}` : ''}</p>
      {t.aliases?.length > 0 && <p className="text-xs text-gray-400 truncate">also: {t.aliases.join(', ')}</p>}
      {t.sample_episodes?.map(e => <p key={e} className="text-[11px] text-gray-500 truncate" title={e}>“{e}”</p>)}
    </div>
  );

  const btn = 'px-2.5 py-1 text-xs rounded-lg border border-gray-300 text-gray-700 hover:bg-gray-50 disabled:opacity-50';
  return (
    <div className="bg-white rounded-2xl border border-gray-200 p-4 space-y-3">
      <span className={`text-xs px-1.5 py-0.5 rounded ${reason.bg}`}>{reason.label}</span>
      <div className="grid grid-cols-2 gap-4">{side(s.a)}{side(s.b)}</div>
      <div className="flex flex-wrap gap-1.5">
        <button disabled={busy} onClick={() => merge(s.a, s.b)} className={btn}>Same — keep “{s.a.name}”</button>
        <button disabled={busy} onClick={() => merge(s.b, s.a)} className={btn}>Same — keep “{s.b.name}”</button>
        <button disabled={busy} onClick={() => act(() => putUnder(s.b.tag_id, s.a.tag_id))} className={btn}>
          “{s.b.name}” is under “{s.a.name}”</button>
        <button disabled={busy} onClick={() => act(() => putUnder(s.a.tag_id, s.b.tag_id))} className={btn}>
          “{s.a.name}” is under “{s.b.name}”</button>
        <button disabled={busy} onClick={() => act(() => markDifferent(s.a.tag_id, s.b.tag_id))} className={btn}>Different</button>
        <button disabled={busy} onClick={() => onSkip(s)} className="px-2.5 py-1 text-xs text-gray-400 hover:text-gray-600">Skip</button>
      </div>
      {error && <p className="text-xs text-red-500">{error}</p>}
    </div>
  );
}

const PAGE = 40;

function timeAgo(iso) {
  if (!iso) return 'never';
  const mins = Math.round((Date.now() - new Date(iso + (iso.endsWith('Z') ? '' : 'Z')).getTime()) / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins} min ago`;
  const hrs = Math.round(mins / 60);
  return hrs < 48 ? `${hrs} h ago` : `${Math.round(hrs / 24)} days ago`;
}

export default function TopicSuggestions({ onChanged }) {
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [computedAt, setComputedAt] = useState(null);
  const [shown, setShown] = useState(PAGE);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [hidden, setHidden] = useState(new Set());
  const [gone, setGone] = useState(new Set());       // topics merged away this session
  const [skipped, setSkipped] = useState(new Set());

  const load = useCallback((quiet = false, count = PAGE) => {
    if (!quiet) setLoading(true);
    call(`${API}/topics-suggestions?limit=${count}`)
      .then(d => { setItems(d.items || []); setTotal(d.total || 0); setComputedAt(d.computed_at); setError(''); setHidden(new Set()); })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);
  useEffect(() => { load(false, shown); }, [load]);   // eslint-disable-line react-hooks/exhaustive-deps

  const recompute = async () => {
    setNotice('');
    try {
      await call(`${API}/topics-suggestions/refresh`, { method: 'POST' });
      setNotice('Recomputing — this takes a few seconds. Refresh to see the new queue.');
    } catch (e) { setError(e.message); }
  };

  const key = (s) => `${s.tag_a}-${s.tag_b}`;
  const done = (s, droppedId, stale = false) => {
    setHidden(prev => new Set(prev).add(key(s)));
    if (droppedId) setGone(prev => new Set(prev).add(droppedId));
    if (!stale) onChanged?.();
    load(true, shown);
  };
  const skip = (s) => setSkipped(prev => new Set(prev).add(key(s)));
  const more = () => { const n = shown + PAGE; setShown(n); load(true, n); };
  const visible = items.filter(s => !hidden.has(key(s)) && !skipped.has(key(s))
                                 && !gone.has(s.tag_a) && !gone.has(s.tag_b));

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div>
          <AdminListCount total={total} shown={items.length} noun="possible duplicate" className="mb-0" />
          <p className="text-xs text-gray-400">
            Same words reordered, spelling variants, and acronyms the episodes use · most episodes first · computed {timeAgo(computedAt)}.
          </p>
        </div>
        <div className="flex gap-3">
          <button onClick={recompute} className="text-xs text-gray-500 hover:underline">Recompute</button>
          <button onClick={() => { setSkipped(new Set()); load(false, shown); }} className="text-xs text-blue-600 hover:underline">Refresh</button>
        </div>
      </div>
      {notice && <p className="text-xs text-green-700 bg-green-50 rounded-lg px-3 py-2">{notice}</p>}
      {loading ? <p className="text-sm text-gray-400">Loading…</p>
        : error ? <p className="text-sm text-red-500">{error}</p>
        : visible.length === 0 ? <p className="text-sm text-gray-400">Nothing left here — Refresh or Load more.</p>
        : visible.map(s => <Card key={key(s)} s={s} onAction={done} onSkip={skip} />)}
      {!loading && !error && items.length < total && (
        <button onClick={more} className="w-full py-2 text-sm text-blue-600 hover:bg-white rounded-lg">
          Load more ({Math.min(PAGE, total - items.length)} of {total - items.length} remaining)
        </button>
      )}
    </div>
  );
}
