import { useState, useEffect, useCallback, useRef } from 'react';
import { API_BASE_URL } from '../config';
import { adminFetch } from '../adminAuth';
import AdminHeader from './AdminHeader';
import { formatDateOnly } from '../adminUtils';

const API = `${API_BASE_URL}/api/admin`;
const PAGE_SIZE = 50;

const SORTS = [
  { id: 'newest',       label: 'Newest first' },
  { id: 'oldest',       label: 'Oldest first' },
  { id: 'credits_desc', label: 'Most credits' },
  { id: 'credits_asc',  label: 'Fewest credits' },
  { id: 'title_asc',    label: 'Title A–Z' },
];

const SOURCE_BADGE = {
  apple_verified:      { label: 'Apple',    bg: 'bg-green-100 text-green-700' },
  approved_suggestion: { label: 'Approved', bg: 'bg-blue-100 text-blue-700' },
  itunes_artist:       { label: 'iTunes',   bg: 'bg-purple-100 text-purple-700' },
  parsed_desc:         { label: 'Parsed',   bg: 'bg-yellow-100 text-yellow-700' },
  parsed_title:        { label: 'Parsed',   bg: 'bg-yellow-100 text-yellow-700' },
  manual:              { label: 'Manual',   bg: 'bg-gray-100 text-gray-600' },
};

// ─── Right panel: Episode details + credit management ─────────────────────────
function EpisodePanel({ episodeId, onChanged }) {
  const [episode, setEpisode] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [personQ, setPersonQ] = useState('');
  const [personResults, setPersonResults] = useState([]);
  const [addingHostId, setAddingHostId] = useState(null);
  const [removingHostId, setRemovingHostId] = useState(null);

  const fetchEpisode = useCallback(async () => {
    if (!episodeId) { setEpisode(null); return; }
    setLoading(true);
    setError('');
    try {
      const res = await adminFetch(`${API}/episodes/${episodeId}`);
      if (!res.ok) throw new Error(`API error ${res.status}`);
      setEpisode(await res.json());
    } catch (e) {
      setError(e.message || 'Failed to load episode');
    } finally {
      setLoading(false);
    }
  }, [episodeId]);

  useEffect(() => {
    fetchEpisode();
    setPersonQ('');
    setPersonResults([]);
  }, [fetchEpisode]);

  useEffect(() => {
    if (!personQ.trim()) { setPersonResults([]); return; }
    adminFetch(`${API}/people?q=${encodeURIComponent(personQ.trim())}&sort=name_asc`)
      .then(r => r.json())
      .then(data => setPersonResults((data.items || []).slice(0, 8)))
      .catch(() => {});
  }, [personQ]);

  const handleAddCredit = async (hostId, isGuest) => {
    setAddingHostId(hostId);
    try {
      const res = await adminFetch(`${API}/episodes/${episodeId}/credits`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ host_id: hostId, is_guest: isGuest }),
      });
      if (!res.ok) throw new Error((await res.json()).detail || 'Failed to add credit');
      setPersonQ('');
      setPersonResults([]);
      await fetchEpisode();
      onChanged?.();
    } catch (e) {
      setError(e.message);
    } finally {
      setAddingHostId(null);
    }
  };

  const handleRemoveCredit = async (hostId) => {
    setRemovingHostId(hostId);
    try {
      const res = await adminFetch(`${API}/episodes/${episodeId}/credits/${hostId}`, { method: 'DELETE' });
      if (!res.ok) throw new Error((await res.json()).detail || 'Failed to remove credit');
      await fetchEpisode();
      onChanged?.();
    } catch (e) {
      setError(e.message);
    } finally {
      setRemovingHostId(null);
    }
  };

  if (!episodeId) {
    return (
      <div className="bg-white rounded-2xl border border-gray-200 p-6 text-center text-sm text-gray-400" style={{ position: "sticky", top: "24px" }}>
        Select an episode to manage its hosts and guests
      </div>
    );
  }

  return (
    <div className="bg-white rounded-2xl border border-gray-200 p-6 overflow-y-auto" style={{ maxHeight: "calc(100vh - 80px)", position: "sticky", top: "24px" }}>
      {loading && !episode ? (
        <div className="py-8 text-center text-gray-400 text-sm">Loading...</div>
      ) : episode ? (
        <>
          <div className="flex items-start gap-3 mb-4">
            {episode.cover_art_url && (
              <img src={episode.cover_art_url} alt={episode.podcast_title}
                className="w-12 h-12 rounded-lg flex-shrink-0 object-cover" />
            )}
            <div className="min-w-0">
              <a href={`/admin/shows?apple_podcast_id=${episode.apple_podcast_id}`}
                className="text-xs font-semibold text-gray-400 hover:text-teal-600 uppercase tracking-wide">
                {episode.podcast_title}
              </a>
              <h2 className="text-sm font-semibold text-gray-900 leading-snug">{episode.title}</h2>
              {episode.published_date && (
                <p className="text-xs text-gray-400 mt-0.5">{formatDateOnly(episode.published_date)}</p>
              )}
            </div>
          </div>

          {/* Current credits */}
          <div className="mb-5">
            <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
              Hosts &amp; Guests ({episode.credits.length})
            </p>
            {episode.credits.length === 0 ? (
              <p className="text-sm text-gray-400 italic">No credits yet</p>
            ) : (
              <div className="space-y-1.5">
                {episode.credits.map(c => {
                  const badge = SOURCE_BADGE[c.data_source] || SOURCE_BADGE.manual;
                  return (
                    <div key={c.host_id} className="flex items-center gap-2 py-1.5 px-3 bg-gray-50 rounded-lg text-sm">
                      {c.profile_image_url ? (
                        <img src={c.profile_image_url} alt={c.name}
                          className="w-6 h-6 rounded-full object-cover flex-shrink-0"
                          onError={e => { e.target.style.display = 'none'; }} />
                      ) : (
                        <span className={`w-2 h-2 rounded-full flex-shrink-0 ${c.is_guest ? 'bg-blue-400' : 'bg-green-500'}`} />
                      )}
                      <a href={`/admin/people?host_id=${c.host_id}`}
                        className="font-medium text-gray-800 hover:text-teal-600 flex-1 truncate">
                        {c.name}
                      </a>
                      <span className="text-gray-400 text-xs flex-shrink-0">{c.is_guest ? 'Guest' : 'Host'}</span>
                      <span className={`text-xs px-1.5 py-0.5 rounded flex-shrink-0 ${badge.bg}`}>{badge.label}</span>
                      <button
                        onClick={() => handleRemoveCredit(c.host_id)}
                        disabled={removingHostId === c.host_id}
                        className="text-gray-300 hover:text-red-500 disabled:opacity-40 flex-shrink-0 text-xs px-1"
                        title="Unlink"
                      >
                        {removingHostId === c.host_id ? '…' : '✕'}
                      </button>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* Add a host/guest */}
          <div className="mb-5">
            <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">Add Host or Guest</p>
            <input
              type="text"
              value={personQ}
              onChange={e => setPersonQ(e.target.value)}
              placeholder="Search people by name..."
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-teal-500 focus:outline-none"
            />
            {personResults.length > 0 && (
              <div className="mt-2 border border-gray-100 rounded-lg divide-y divide-gray-50 overflow-hidden">
                {personResults.map(p => (
                  <div key={p.host_id} className="flex items-center gap-2 px-3 py-2 text-sm">
                    <span className="flex-1 truncate text-gray-800">{p.full_name}</span>
                    <button
                      onClick={() => handleAddCredit(p.host_id, false)}
                      disabled={addingHostId === p.host_id}
                      className="text-xs px-2 py-1 rounded bg-green-50 text-green-700 hover:bg-green-100 disabled:opacity-40"
                    >
                      + Host
                    </button>
                    <button
                      onClick={() => handleAddCredit(p.host_id, true)}
                      disabled={addingHostId === p.host_id}
                      className="text-xs px-2 py-1 rounded bg-blue-50 text-blue-700 hover:bg-blue-100 disabled:opacity-40"
                    >
                      + Guest
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Episode description */}
          {episode.description && (
            <div className="mb-5 border-t border-gray-100 pt-4">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">Episode Description</p>
              <p className="text-sm text-gray-700 leading-relaxed whitespace-pre-wrap">{episode.description}</p>
            </div>
          )}

          {error && <p className="text-sm text-red-600 mt-3">{error}</p>}
        </>
      ) : null}
    </div>
  );
}

// ─── Main page ─────────────────────────────────────────────────────────────────
export default function AdminEpisodes() {
  const [episodes, setEpisodes] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [listError, setListError] = useState(null);
  const [searchQ, setSearchQ] = useState('');
  const [showFilter, setShowFilter] = useState(() => new URLSearchParams(window.location.search).get('show') || '');
  const [showOptions, setShowOptions] = useState([]);
  const [sort, setSort] = useState('newest');
  // Deep-link support: /admin/episodes?episode_id=X auto-opens that episode
  const [selectedId, setSelectedId] = useState(() => {
    const id = new URLSearchParams(window.location.search).get('episode_id');
    return id ? parseInt(id, 10) : null;
  });
  const searchRef = useRef(null);

  const fetchEpisodes = useCallback(async (q, show, s, offset, append) => {
    setLoading(true);
    setListError(null);
    try {
      const params = new URLSearchParams({ q, show, sort: s, limit: PAGE_SIZE, offset });
      const res = await adminFetch(`${API}/episodes?${params}`);
      if (!res.ok) throw new Error(`API error ${res.status}`);
      const data = await res.json();
      setEpisodes(prev => append ? [...prev, ...data.items] : data.items);
      setTotal(data.total);
    } catch (e) {
      setListError(e.message || 'Failed to load');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchEpisodes(searchQ, showFilter, sort, 0, false); }, [fetchEpisodes]);

  useEffect(() => {
    adminFetch(`${API}/shows`)
      .then(r => r.json())
      .then(shows => setShowOptions(shows.map(s => s.podcast_title).sort()))
      .catch(() => {});
  }, []);

  const handleSearch = (e) => {
    const q = e.target.value;
    setSearchQ(q);
    fetchEpisodes(q, showFilter, sort, 0, false);
  };

  const handleShowFilter = (show) => {
    setShowFilter(show);
    fetchEpisodes(searchQ, show, sort, 0, false);
  };

  const handleSort = (s) => {
    setSort(s);
    fetchEpisodes(searchQ, showFilter, s, 0, false);
  };

  const handleLoadMore = () => {
    fetchEpisodes(searchQ, showFilter, sort, episodes.length, true);
  };

  const handleChanged = () => {
    fetchEpisodes(searchQ, showFilter, sort, 0, false);
  };

  return (
    <div className="min-h-screen bg-gray-100 font-sans">
      <AdminHeader active="Episodes" right={<span className="text-sm text-gray-400">{total} episodes</span>} />

      <div className="flex gap-6 p-6 max-w-7xl mx-auto">

        {/* LEFT — episode list */}
        <div className="flex-1 min-w-0">

          {/* Search + filters */}
          <div className="bg-white rounded-2xl border border-gray-200 p-4 mb-4">
            <div className="relative mb-3">
              <input ref={searchRef} type="text" value={searchQ} onChange={handleSearch}
                placeholder="Search by episode or show title..."
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-teal-500 focus:outline-none pr-7" />
              {searchQ && (
                <button
                  onClick={() => { setSearchQ(''); fetchEpisodes('', showFilter, sort, 0, false); }}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600 text-lg leading-none"
                >×</button>
              )}
            </div>

            <div className="flex items-center gap-2 flex-wrap">
              <select value={showFilter} onChange={e => handleShowFilter(e.target.value)}
                className="text-xs border border-gray-200 rounded px-2 py-1 focus:outline-none focus:border-teal-400">
                <option value="">All Shows</option>
                {showOptions.map(s => <option key={s} value={s}>{s}</option>)}
              </select>
              <span className="text-xs text-gray-400">Sort:</span>
              <select value={sort} onChange={e => handleSort(e.target.value)}
                className="text-xs border border-gray-200 rounded px-2 py-1 focus:outline-none focus:border-teal-400">
                {SORTS.map(s => <option key={s.id} value={s.id}>{s.label}</option>)}
              </select>
            </div>
          </div>

          {/* Episode list */}
          <div className="bg-white rounded-2xl border border-gray-200 overflow-hidden" style={{ maxHeight: "calc(100vh - 280px)", overflowY: "auto" }}>
            {loading && episodes.length === 0 ? (
              <div className="py-12 text-center text-gray-400 text-sm">Loading...</div>
            ) : listError ? (
              <div className="py-12 flex flex-col items-center gap-3">
                <p className="text-sm font-semibold text-red-500">Couldn't load episodes</p>
                <p className="text-xs text-gray-500">{listError}</p>
                <button
                  className="px-3 py-1.5 bg-teal-600 text-white text-sm rounded hover:bg-teal-700"
                  onClick={() => fetchEpisodes(searchQ, showFilter, sort, 0, false)}
                >
                  Retry
                </button>
              </div>
            ) : episodes.length === 0 ? (
              <div className="py-12 text-center text-gray-400 text-sm">No episodes found</div>
            ) : (
              <>
                <div className="divide-y divide-gray-100">
                  {episodes.map(ep => {
                    const isSelected = selectedId === ep.episode_id;
                    return (
                      <div key={ep.episode_id}
                        className={`flex items-center gap-3 px-4 py-3 hover:bg-gray-50 cursor-pointer transition-colors ${
                          isSelected ? 'bg-teal-50 border-l-2 border-teal-500' : ''
                        }`}
                        onClick={() => setSelectedId(isSelected ? null : ep.episode_id)}>
                        {ep.cover_art_url && (
                          <img src={ep.cover_art_url} alt={ep.podcast_title}
                            className="w-8 h-8 rounded object-cover flex-shrink-0" />
                        )}
                        <div className="flex-1 min-w-0">
                          <p className="text-sm font-medium text-gray-900 truncate">{ep.title}</p>
                          <p className="text-xs text-gray-400 truncate">
                            <a href={`/admin/shows?apple_podcast_id=${ep.apple_podcast_id}`}
                              onClick={e => e.stopPropagation()}
                              className="hover:text-teal-600">
                              {ep.podcast_title}
                            </a>
                            {ep.published_date && ` · ${formatDateOnly(ep.published_date)}`}
                            {' · '}{ep.credit_count} credit{ep.credit_count !== 1 ? 's' : ''}
                          </p>
                        </div>
                      </div>
                    );
                  })}
                </div>
                {episodes.length < total && (
                  <div className="p-3 text-center">
                    <button
                      onClick={handleLoadMore}
                      disabled={loading}
                      className="px-4 py-1.5 bg-gray-100 hover:bg-gray-200 disabled:opacity-50 text-gray-700 text-sm font-medium rounded-lg"
                    >
                      {loading ? 'Loading…' : `Load more (${total - episodes.length} remaining)`}
                    </button>
                  </div>
                )}
              </>
            )}
          </div>
        </div>

        {/* RIGHT — episode details + credits */}
        <div className="w-96 flex-shrink-0">
          <EpisodePanel episodeId={selectedId} onChanged={handleChanged} />
        </div>
      </div>
    </div>
  );
}
