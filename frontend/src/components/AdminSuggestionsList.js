import { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { API_BASE_URL } from '../config';
import { adminFetch } from '../adminAuth';
import AdminHeader from './AdminHeader';
import { formatDateOnly, formatSuggestionSource, highlightNames } from '../adminUtils';

const API = `${API_BASE_URL}/api/admin`;
const PAGE_SIZE = 50;

const SORTS = [
  { id: 'newest',   label: 'Newest first' },
  { id: 'oldest',   label: 'Oldest first' },
  { id: 'name_asc', label: 'Name A–Z' },
];

// Browse/filter view over the suggestions queue, with quick inline
// approve/reject per row so a batch can be scanned and cleared without
// leaving the list — full context (existing credits, name override) is
// still one click away via the review page each row links to.
export default function AdminSuggestionsList() {
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [listError, setListError] = useState(null);
  const [actioningId, setActioningId] = useState(null);
  const [actionError, setActionError] = useState(null);
  const [bulk, setBulk] = useState(null); // { done, total, failed } while Approve All is running
  const bulkStopRef = useRef(false);
  // suggestion_id -> corrected name, e.g. "Elin Bergman COO" -> "Elin Bergman".
  // Kept keyed by id (not row-local state) so an edit survives a re-render
  // and so Approve All can pick it up too, not just the single-row button.
  const [editedNames, setEditedNames] = useState({});
  const [editingId, setEditingId] = useState(null);
  // approve/reject both sweep EVERY pending suggestion sharing the same
  // candidate_name (case-insensitive) on the backend — one name showing up
  // on several episodes is common, and resolving one row there resolves
  // all of them at once, server-side, regardless of which is visible here.
  // Tracked in a ref (not state) so the async Approve All loop always reads
  // the latest set without waiting on a re-render.
  const resolvedNamesRef = useRef(new Set());
  const [searchQ, setSearchQ] = useState('');
  const [showFilter, setShowFilter] = useState(
    () => new URLSearchParams(window.location.search).get('apple_podcast_id') || ''
  );
  const [showOptions, setShowOptions] = useState([]);
  const [source, setSource] = useState('');
  const [sourceOptions, setSourceOptions] = useState([]);
  const [sort, setSort] = useState('newest');
  const searchRef = useRef(null);

  const fetchSources = useCallback((show) => {
    const params = new URLSearchParams();
    if (show) params.set('apple_podcast_id', show);
    adminFetch(`${API}/suggestions/sources?${params}`)
      .then(r => r.json())
      .then(data => setSourceOptions(Array.isArray(data) ? data : []))
      .catch(() => {});
  }, []);

  const fetchItems = useCallback(async (q, show, src, s, offset, append) => {
    setLoading(true);
    setListError(null);
    try {
      const params = new URLSearchParams({ search: q, sort: s, limit: PAGE_SIZE, offset });
      if (show) params.set('apple_podcast_id', show);
      if (src) params.set('source', src);
      const res = await adminFetch(`${API}/suggestions/list?${params}`);
      if (!res.ok) throw new Error(`API error ${res.status}`);
      const data = await res.json();
      setItems(prev => append ? [...prev, ...data.items] : data.items);
      setTotal(data.total);
    } catch (e) {
      setListError(e.message || 'Failed to load');
    } finally {
      setLoading(false);
    }
  }, []);

  // Runs once: filters are applied by the controls themselves, so depending
  // on them here would refetch per keystroke.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => { fetchItems(searchQ, showFilter, source, sort, 0, false); }, [fetchItems]);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => { fetchSources(showFilter); }, [fetchSources]);

  useEffect(() => {
    adminFetch(`${API}/shows`)
      .then(r => r.json())
      .then(data => setShowOptions(Array.isArray(data) ? data : []))
      .catch(() => {});
  }, []);

  const handleSearch = (e) => {
    const q = e.target.value;
    setSearchQ(q);
    fetchItems(q, showFilter, source, sort, 0, false);
  };

  const handleShowFilter = (show) => {
    setShowFilter(show);
    setSource('');
    fetchSources(show);
    fetchItems(searchQ, show, '', sort, 0, false);
    const params = new URLSearchParams(window.location.search);
    if (show) params.set('apple_podcast_id', show); else params.delete('apple_podcast_id');
    window.history.replaceState({}, '', `${window.location.pathname}${params.toString() ? `?${params}` : ''}`);
  };

  const handleSourceFilter = (src) => {
    setSource(src);
    fetchItems(searchQ, showFilter, src, sort, 0, false);
  };

  const handleSort = (s) => {
    setSort(s);
    fetchItems(searchQ, showFilter, source, s, 0, false);
  };

  const handleLoadMore = () => {
    fetchItems(searchQ, showFilter, source, sort, items.length, true);
  };

  // Removes the row locally on success rather than refetching the whole
  // page — scanning a long list and clearing rows one by one shouldn't
  // reset scroll position or re-request everything after each click. Also
  // decrements the show/pattern dropdown counts, which otherwise go stale
  // the moment the first row in a filtered batch is resolved — they're
  // fetched once on mount/filter-change, not on every action.
  const removeItem = (item) => {
    const { suggestion_id: suggestionId, apple_podcast_id, source: itemSource } = item;
    setItems(prev => prev.filter(i => i.suggestion_id !== suggestionId));
    setTotal(t => Math.max(0, t - 1));
    setEditedNames(prev => {
      if (!(suggestionId in prev)) return prev;
      const next = { ...prev };
      delete next[suggestionId];
      return next;
    });
    setShowOptions(prev => prev.map(s => s.apple_podcast_id === apple_podcast_id
      ? { ...s, pending_suggestion_count: Math.max(0, (s.pending_suggestion_count || 0) - 1) }
      : s));
    setSourceOptions(prev => prev.map(s => s.source === itemSource
      ? { ...s, count: Math.max(0, s.count - 1) }
      : s));
  };

  // approve_suggestion accepts an optional {name} override — an edited row
  // sends its corrected name, an untouched row sends no body at all so the
  // scanner's original candidate_name is used exactly as before.
  const approveOptions = (item) => {
    const edited = editedNames[item.suggestion_id]?.trim();
    const body = edited && edited !== item.candidate_name ? JSON.stringify({ name: edited }) : undefined;
    return { method: 'POST', ...(body && { body, headers: { 'Content-Type': 'application/json' } }) };
  };

  const startEditing = (item) => {
    setEditedNames(prev => (item.suggestion_id in prev ? prev : { ...prev, [item.suggestion_id]: item.candidate_name }));
    setEditingId(item.suggestion_id);
  };

  const isAlreadyResolved = (item) => resolvedNamesRef.current.has(item.candidate_name.toLowerCase());

  // Records both spellings — the row's original name and any override typed
  // in for it — so a later row for either spelling is recognized as already
  // handled instead of round-tripping to a 404.
  const markResolved = (item) => {
    resolvedNamesRef.current.add(item.candidate_name.toLowerCase());
    const edited = editedNames[item.suggestion_id]?.trim();
    if (edited) resolvedNamesRef.current.add(edited.toLowerCase());
  };

  const handleReject = async (item) => {
    if (isAlreadyResolved(item)) { removeItem(item); return; }
    setActioningId(item.suggestion_id);
    setActionError(null);
    try {
      const res = await adminFetch(`${API}/suggestions/${item.suggestion_id}/reject`, { method: 'POST' });
      // 404 here means "already reviewed" — another row for the same name
      // was resolved first and the backend's own name-based sweep already
      // caught this one. That's the desired outcome, not a failure.
      if (res.status === 404) { markResolved(item); removeItem(item); return; }
      if (!res.ok) throw new Error(`API error ${res.status}`);
      markResolved(item);
      removeItem(item);
    } catch (e) {
      setActionError({ id: item.suggestion_id, message: e.message || 'Failed to reject' });
    } finally {
      setActioningId(null);
    }
  };

  const handleApprove = async (item) => {
    if (isAlreadyResolved(item)) { removeItem(item); return; }
    setActioningId(item.suggestion_id);
    setActionError(null);
    try {
      const res = await adminFetch(`${API}/suggestions/${item.suggestion_id}/approve`, approveOptions(item));
      if (res.status === 404) { markResolved(item); removeItem(item); return; }
      if (!res.ok) throw new Error(`API error ${res.status}`);
      markResolved(item);
      removeItem(item);
    } catch (e) {
      setActionError({ id: item.suggestion_id, message: e.message || 'Failed to approve' });
    } finally {
      setActioningId(null);
    }
  };

  // Approves every currently-loaded row, one at a time — each approval also
  // scans the whole archive for other episodes naming that person, so this
  // is genuinely slow (~5s/row) rather than a client-side limitation.
  // Sequential on purpose: this hits the same free-tier Postgres instance
  // as everything else, and running 50 of these concurrently would be a
  // much heavier spike than the review queue normally puts on it.
  const handleApproveAll = async () => {
    const targets = [...items];
    if (!targets.length) return;
    if (!window.confirm(
      `Approve all ${targets.length} visible suggestion${targets.length === 1 ? '' : 's'}? ` +
      `This can take a few minutes (~5s each) and can't be undone in bulk.`
    )) return;

    bulkStopRef.current = false;
    setBulk({ done: 0, total: targets.length, failed: 0 });

    for (const item of targets) {
      if (bulkStopRef.current) break;
      if (isAlreadyResolved(item)) {
        removeItem(item);
        setBulk(prev => prev && { ...prev, done: prev.done + 1 });
        continue;
      }
      try {
        const res = await adminFetch(`${API}/suggestions/${item.suggestion_id}/approve`, approveOptions(item));
        if (res.status === 404) {
          markResolved(item);
          removeItem(item);
          setBulk(prev => prev && { ...prev, done: prev.done + 1 });
          continue;
        }
        if (!res.ok) throw new Error(`API error ${res.status}`);
        markResolved(item);
        removeItem(item);
        setBulk(prev => prev && { ...prev, done: prev.done + 1 });
      } catch (e) {
        setActionError({ id: item.suggestion_id, message: e.message || 'Failed to approve' });
        setBulk(prev => prev && { ...prev, done: prev.done + 1, failed: prev.failed + 1 });
      }
    }
    setBulk(null);
  };

  const handleStopBulk = () => { bulkStopRef.current = true; };

  // How many *other* visible rows share this exact name — shown as a badge
  // so it's clear why resolving one row can make several others vanish at
  // once (they're the same person, same backend name-based sweep).
  const nameCounts = useMemo(() => {
    const counts = {};
    for (const item of items) {
      const key = item.candidate_name.toLowerCase();
      counts[key] = (counts[key] || 0) + 1;
    }
    return counts;
  }, [items]);

  return (
    <div className="min-h-screen bg-gray-100 font-sans">
      <AdminHeader
        active="Suggestions"
        right={<span className="text-sm text-gray-400">{total} pending</span>}
      />

      <div className="px-6 py-3 bg-white border-b border-gray-200 flex items-center gap-2">
        <a href="/admin" className="text-sm text-teal-700 hover:text-teal-900 hover:underline">
          ← Back to review queue
        </a>
      </div>

      <div className="p-4 md:p-6 max-w-6xl mx-auto">

        {/* Filters */}
        <div className="bg-white rounded-2xl border border-gray-200 p-4 mb-4">
          <div className="relative mb-3">
            <input ref={searchRef} type="text" value={searchQ} onChange={handleSearch} disabled={!!bulk}
              placeholder="Search by candidate name..."
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-teal-500 focus:outline-none pr-7 disabled:bg-gray-50" />
            {searchQ && (
              <button
                onClick={() => { setSearchQ(''); fetchItems('', showFilter, source, sort, 0, false); }}
                disabled={!!bulk}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600 text-lg leading-none"
              >×</button>
            )}
          </div>

          <div className="flex items-center gap-2 flex-wrap">
            <select value={showFilter} onChange={e => handleShowFilter(e.target.value)} disabled={!!bulk}
              className="text-xs border border-gray-200 rounded px-2 py-1 focus:outline-none focus:border-teal-400 max-w-[14rem] disabled:bg-gray-50">
              <option value="">All Shows</option>
              {showOptions.map(s => (
                <option key={s.apple_podcast_id} value={s.apple_podcast_id}>
                  {s.podcast_title}{s.pending_suggestion_count > 0 ? ` (${s.pending_suggestion_count})` : ''}
                </option>
              ))}
            </select>

            <select value={source} onChange={e => handleSourceFilter(e.target.value)} disabled={!!bulk}
              className="text-xs border border-gray-200 rounded px-2 py-1 focus:outline-none focus:border-teal-400 max-w-[16rem] disabled:bg-gray-50">
              <option value="">All Patterns</option>
              {sourceOptions.map(({ source: src, count }) => {
                const { scope, pattern } = formatSuggestionSource(src);
                return (
                  <option key={src} value={src}>
                    {scope}{pattern ? ` · ${pattern}` : ''} ({count})
                  </option>
                );
              })}
            </select>

            <span className="text-xs text-gray-400">Sort:</span>
            <select value={sort} onChange={e => handleSort(e.target.value)} disabled={!!bulk}
              className="text-xs border border-gray-200 rounded px-2 py-1 focus:outline-none focus:border-teal-400 disabled:bg-gray-50">
              {SORTS.map(s => <option key={s.id} value={s.id}>{s.label}</option>)}
            </select>
          </div>
        </div>

        {/* Bulk approve */}
        {items.length > 0 && (
          <div className="bg-white rounded-2xl border border-gray-200 p-4 mb-4">
            {bulk ? (
              <div>
                <div className="flex items-center justify-between mb-2">
                  <p className="text-sm text-gray-600">
                    Approving {bulk.done} / {bulk.total}
                    {bulk.failed > 0 && <span className="text-red-500"> ({bulk.failed} failed)</span>}
                  </p>
                  <button
                    onClick={handleStopBulk}
                    className="px-3 py-1 rounded text-xs font-medium bg-gray-100 text-gray-700 hover:bg-gray-200"
                  >
                    Stop
                  </button>
                </div>
                <div className="w-full h-2 bg-gray-100 rounded-full overflow-hidden">
                  <div
                    className="h-full bg-teal-500 transition-all duration-300"
                    style={{ width: `${(bulk.done / bulk.total) * 100}%` }}
                  />
                </div>
              </div>
            ) : (
              <button
                onClick={handleApproveAll}
                className="px-3 py-1.5 rounded-lg text-sm font-medium bg-green-600 text-white hover:bg-green-700"
              >
                Approve All ({items.length} visible)
              </button>
            )}
          </div>
        )}

        {/* List */}
        <div className="bg-white rounded-2xl border border-gray-200 overflow-hidden">
          {loading && items.length === 0 ? (
            <div className="py-12 text-center text-gray-400 text-sm">Loading...</div>
          ) : listError ? (
            <div className="py-12 flex flex-col items-center gap-3">
              <p className="text-sm font-semibold text-red-500">Couldn't load suggestions</p>
              <p className="text-xs text-gray-500">{listError}</p>
              <button
                className="px-3 py-1.5 bg-teal-600 text-white text-sm rounded hover:bg-teal-700"
                onClick={() => fetchItems(searchQ, showFilter, source, sort, 0, false)}
              >
                Retry
              </button>
            </div>
          ) : items.length === 0 ? (
            <div className="py-12 flex flex-col items-center gap-3">
              <p className="text-sm text-gray-400">No pending suggestions match these filters</p>
              <button
                onClick={() => fetchItems(searchQ, showFilter, source, sort, 0, false)}
                disabled={loading}
                className="px-3 py-1.5 bg-teal-600 text-white text-sm font-medium rounded-lg hover:bg-teal-700 disabled:opacity-50"
              >
                {loading ? 'Loading…' : 'Refresh'}
              </button>
            </div>
          ) : (
            <>
              <div className="divide-y divide-gray-100">
                {items.map(item => {
                  const { scope, pattern } = formatSuggestionSource(item.source);
                  const isTitle = scope === 'Episode Title';
                  const isActioning = actioningId === item.suggestion_id;
                  const isEditing = editingId === item.suggestion_id;
                  const hasEdit = item.suggestion_id in editedNames && editedNames[item.suggestion_id].trim() !== item.candidate_name;
                  const displayName = editedNames[item.suggestion_id] ?? item.candidate_name;
                  const dupCount = nameCounts[item.candidate_name.toLowerCase()];
                  return (
                    <div key={item.suggestion_id} className="px-4 py-3 hover:bg-gray-50 transition-colors">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0 flex-1">
                          {isEditing ? (
                            <input
                              autoFocus
                              type="text"
                              value={editedNames[item.suggestion_id] ?? item.candidate_name}
                              onChange={e => setEditedNames(prev => ({ ...prev, [item.suggestion_id]: e.target.value }))}
                              onBlur={() => setEditingId(null)}
                              onKeyDown={e => {
                                if (e.key === 'Enter') { e.preventDefault(); setEditingId(null); }
                                if (e.key === 'Escape') {
                                  setEditedNames(prev => {
                                    const next = { ...prev };
                                    delete next[item.suggestion_id];
                                    return next;
                                  });
                                  setEditingId(null);
                                }
                              }}
                              className="text-sm font-semibold text-gray-900 border border-teal-400 rounded px-1 -mx-1 focus:outline-none w-full max-w-xs"
                            />
                          ) : (
                            <button
                              onClick={() => startEditing(item)}
                              disabled={isActioning || !!bulk}
                              title="Click to edit name"
                              className={`text-sm font-semibold truncate text-left hover:underline decoration-dotted ${
                                hasEdit ? 'text-teal-700' : 'text-gray-900'
                              }`}
                            >
                              {displayName}{hasEdit && <span className="text-xs text-teal-500 font-normal"> (edited)</span>}
                            </button>
                          )}
                          {!isEditing && dupCount > 1 && (
                            <span
                              title={`${dupCount} visible rows share this name — resolving one resolves all of them`}
                              className="ml-1.5 inline-block px-1.5 py-0 rounded-full text-[10px] font-medium bg-amber-100 text-amber-700 align-middle"
                            >
                              ×{dupCount}
                            </span>
                          )}
                          <a href={`/admin?suggestion_id=${item.suggestion_id}`} className="block">
                            <p className="text-xs text-gray-400 truncate mt-0.5">
                              {item.podcast_title} · {item.episode_title}
                              {item.created_at && ` · ${formatDateOnly(item.created_at.slice(0, 10))}`}
                            </p>
                            {item.matched_text && (
                              <p className="text-xs text-gray-500 truncate mt-1 italic">
                                {highlightNames(item.matched_text, [
                                  { name: item.candidate_name, className: 'bg-yellow-100 not-italic font-medium text-gray-900' },
                                ])}
                              </p>
                            )}
                          </a>
                        </div>
                        <div className="flex-shrink-0 flex items-center gap-2">
                          <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium whitespace-nowrap ${
                            isTitle ? 'bg-blue-100 text-blue-700' : 'bg-purple-100 text-purple-700'
                          }`}>
                            {scope}{pattern ? ` · ${pattern}` : ''}
                          </span>
                          <button
                            onClick={() => handleApprove(item)}
                            disabled={isActioning || !!bulk}
                            title={hasEdit ? `Approve as "${editedNames[item.suggestion_id].trim()}"` : 'Approve'}
                            className="px-2 py-1 rounded text-xs font-medium bg-green-100 text-green-700 hover:bg-green-200 disabled:opacity-50"
                          >
                            ✓
                          </button>
                          <button
                            onClick={() => handleReject(item)}
                            disabled={isActioning || !!bulk}
                            title="Reject"
                            className="px-2 py-1 rounded text-xs font-medium bg-red-100 text-red-700 hover:bg-red-200 disabled:opacity-50"
                          >
                            ✕
                          </button>
                        </div>
                      </div>
                      {actionError?.id === item.suggestion_id && (
                        <p className="text-xs text-red-500 mt-1">{actionError.message}</p>
                      )}
                    </div>
                  );
                })}
              </div>
              {items.length < total && (
                <div className="p-3 text-center">
                  <button
                    onClick={handleLoadMore}
                    disabled={loading || !!bulk}
                    className="px-4 py-1.5 bg-gray-100 hover:bg-gray-200 disabled:opacity-50 text-gray-700 text-sm font-medium rounded-lg"
                  >
                    {loading ? 'Loading…' : `Load more (${total - items.length} remaining)`}
                  </button>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
