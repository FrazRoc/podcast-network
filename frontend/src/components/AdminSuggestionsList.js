import { useState, useEffect, useCallback, useRef } from 'react';
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

// Read-only browse/filter view over the suggestions queue — a preview
// surface for a future bulk approve/reject action, so a batch can be seen
// and trusted (or not) before anything is done to it. Acting on a
// suggestion still happens one at a time on the existing review page;
// each row links there rather than duplicating the approve/reject flow.
export default function AdminSuggestionsList() {
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [listError, setListError] = useState(null);
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
            <input ref={searchRef} type="text" value={searchQ} onChange={handleSearch}
              placeholder="Search by candidate name..."
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-teal-500 focus:outline-none pr-7" />
            {searchQ && (
              <button
                onClick={() => { setSearchQ(''); fetchItems('', showFilter, source, sort, 0, false); }}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600 text-lg leading-none"
              >×</button>
            )}
          </div>

          <div className="flex items-center gap-2 flex-wrap">
            <select value={showFilter} onChange={e => handleShowFilter(e.target.value)}
              className="text-xs border border-gray-200 rounded px-2 py-1 focus:outline-none focus:border-teal-400 max-w-[14rem]">
              <option value="">All Shows</option>
              {showOptions.map(s => (
                <option key={s.apple_podcast_id} value={s.apple_podcast_id}>
                  {s.podcast_title}{s.pending_suggestion_count > 0 ? ` (${s.pending_suggestion_count})` : ''}
                </option>
              ))}
            </select>

            <select value={source} onChange={e => handleSourceFilter(e.target.value)}
              className="text-xs border border-gray-200 rounded px-2 py-1 focus:outline-none focus:border-teal-400 max-w-[16rem]">
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
            <select value={sort} onChange={e => handleSort(e.target.value)}
              className="text-xs border border-gray-200 rounded px-2 py-1 focus:outline-none focus:border-teal-400">
              {SORTS.map(s => <option key={s.id} value={s.id}>{s.label}</option>)}
            </select>
          </div>
        </div>

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
            <div className="py-12 text-center text-gray-400 text-sm">No pending suggestions match these filters</div>
          ) : (
            <>
              <div className="divide-y divide-gray-100">
                {items.map(item => {
                  const { scope, pattern } = formatSuggestionSource(item.source);
                  const isTitle = scope === 'Episode Title';
                  return (
                    <a
                      key={item.suggestion_id}
                      href={`/admin?suggestion_id=${item.suggestion_id}`}
                      className="block px-4 py-3 hover:bg-gray-50 transition-colors"
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <p className="text-sm font-semibold text-gray-900 truncate">{item.candidate_name}</p>
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
                        </div>
                        <span className={`flex-shrink-0 inline-block px-2 py-0.5 rounded text-xs font-medium whitespace-nowrap ${
                          isTitle ? 'bg-blue-100 text-blue-700' : 'bg-purple-100 text-purple-700'
                        }`}>
                          {scope}{pattern ? ` · ${pattern}` : ''}
                        </span>
                      </div>
                    </a>
                  );
                })}
              </div>
              {items.length < total && (
                <div className="p-3 text-center">
                  <button
                    onClick={handleLoadMore}
                    disabled={loading}
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
