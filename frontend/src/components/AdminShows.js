import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { API_BASE_URL } from '../config';
import { adminFetch } from '../adminAuth';
import AdminHeader from './AdminHeader';
import { formatDateOnly } from '../adminUtils';

const API = `${API_BASE_URL}/api/admin`;

const STATUS_BADGE = {
  pending:     { label: 'Pending',     bg: 'bg-gray-100 text-gray-600' },
  in_progress: { label: 'In Progress', bg: 'bg-blue-100 text-blue-700' },
  success:     { label: 'Success',     bg: 'bg-green-100 text-green-700' },
  failed:      { label: 'Failed',      bg: 'bg-red-100 text-red-700' },
};

const SOURCE_BADGE = {
  apple_verified:      { label: 'Apple',    bg: 'bg-green-100 text-green-700' },
  approved_suggestion: { label: 'Approved', bg: 'bg-blue-100 text-blue-700' },
  itunes_artist:       { label: 'iTunes',   bg: 'bg-purple-100 text-purple-700' },
  parsed_desc:         { label: 'Parsed',   bg: 'bg-yellow-100 text-yellow-700' },
  parsed_title:        { label: 'Parsed',   bg: 'bg-yellow-100 text-yellow-700' },
  manual:              { label: 'Manual',   bg: 'bg-gray-100 text-gray-600' },
};

const FILTERS = [
  { id: 'all',     label: 'All' },
  { id: 'pending', label: 'Pending' },
  { id: 'success', label: 'Success' },
  { id: 'failed',  label: 'Failed' },
];

const SORTS = [
  { id: 'title_asc',      label: 'Title A–Z' },
  { id: 'episodes_desc',  label: 'Most episodes' },
  { id: 'guests_desc',    label: 'Most guests' },
  { id: 'guests_asc',     label: 'Fewest guests' },
  { id: 'recent_scrape',  label: 'Recently scraped' },
];

// Accepts a raw Apple Podcast ID or a full podcasts.apple.com URL and
// pulls out the numeric ID, e.g. .../id1234567890 -> 1234567890
const extractAppleId = (input) => {
  const trimmed = input.trim();
  const match = trimmed.match(/id(\d+)/) || trimmed.match(/^(\d+)$/);
  return match ? match[1] : trimmed;
};

// ─── Right panel: Add or Edit ──────────────────────────────────────────────────
function ShowPanel({ selected, onDone, onCancel }) {
  const [newShowInput, setNewShowInput] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState('');
  const [episodes, setEpisodes] = useState([]);
  const [episodesTotal, setEpisodesTotal] = useState(0);
  const [hosts, setHosts] = useState([]);
  const [hostQ, setHostQ] = useState('');
  const [hostResults, setHostResults] = useState([]);
  const [addingHostId, setAddingHostId] = useState(null);
  const [removingHostId, setRemovingHostId] = useState(null);
  const isEdit = !!selected;

  const fetchHosts = useCallback(() => {
    if (!selected) { setHosts([]); return; }
    adminFetch(`${API}/shows/${selected.apple_podcast_id}/hosts`)
      .then(r => r.json())
      .then(setHosts)
      .catch(() => {});
  }, [selected]);

  useEffect(() => {
    setNewShowInput('');
    setResult(null);
    setError('');
    setEpisodes([]);
    setEpisodesTotal(0);
    setHostQ('');
    setHostResults([]);
    fetchHosts();
    if (!selected) return;
    adminFetch(`${API}/episodes?show=${encodeURIComponent(selected.podcast_title)}&sort=newest&limit=10`)
      .then(r => r.json())
      .then(data => { setEpisodes(data.items || []); setEpisodesTotal(data.total || 0); })
      .catch(() => {});
  }, [selected, fetchHosts]);

  useEffect(() => {
    if (!hostQ.trim()) { setHostResults([]); return; }
    adminFetch(`${API}/people?q=${encodeURIComponent(hostQ.trim())}&sort=name_asc`)
      .then(r => r.json())
      .then(data => setHostResults((data.items || []).slice(0, 8)))
      .catch(() => {});
  }, [hostQ]);

  const handleAddShowHost = async (hostId) => {
    setAddingHostId(hostId);
    try {
      const res = await adminFetch(`${API}/shows/${selected.apple_podcast_id}/hosts`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ host_id: hostId }),
      });
      if (!res.ok) throw new Error((await res.json()).detail || 'Failed to add host');
      setHostQ('');
      setHostResults([]);
      fetchHosts();
    } catch (e) {
      setError(e.message);
    } finally {
      setAddingHostId(null);
    }
  };

  const handleRemoveShowHost = async (hostId) => {
    setRemovingHostId(hostId);
    try {
      const res = await adminFetch(`${API}/shows/${selected.apple_podcast_id}/hosts/${hostId}`, { method: 'DELETE' });
      if (!res.ok) throw new Error((await res.json()).detail || 'Failed to remove host');
      fetchHosts();
    } catch (e) {
      setError(e.message);
    } finally {
      setRemovingHostId(null);
    }
  };

  const handleAdd = async () => {
    if (!newShowInput.trim() || submitting) return;
    setSubmitting(true);
    setError('');
    setResult(null);
    try {
      const apple_podcast_id = extractAppleId(newShowInput);
      const res = await adminFetch(`${API}/shows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ apple_podcast_id }),
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.detail || 'Failed to add show');
        return;
      }
      // Kick off a scrape immediately, mirroring People's "Create + Scan"
      const scrapeRes = await adminFetch(`${API}/shows/${apple_podcast_id}/scrape-now`, { method: 'POST' });
      const scrapeData = await scrapeRes.json();
      setResult({
        name: data.title,
        scraping: scrapeRes.ok,
        message: scrapeRes.ok ? null : (scrapeData.detail || 'Added, but the scrape could not be started'),
      });
      setNewShowInput('');
      onDone?.();
    } catch (e) {
      setError(e.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleScrapeNow = async () => {
    if (!selected) return;
    setSubmitting(true);
    setError('');
    setResult(null);
    try {
      const res = await adminFetch(`${API}/shows/${selected.apple_podcast_id}/scrape-now`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) {
        setError(data.detail || 'Failed to start scrape');
      } else {
        setResult({ name: selected.podcast_title, scraping: true });
      }
      onDone?.();
    } catch (e) {
      setError(e.message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="bg-white rounded-2xl border border-gray-200 p-6 overflow-y-auto" style={{ maxHeight: "calc(100vh - 80px)", position: "sticky", top: "24px" }}>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-base font-semibold text-gray-900">
          {isEdit ? 'Edit Show' : 'Add New Show'}
        </h2>
        {isEdit && (
          <button onClick={onCancel} className="text-xs text-gray-400 hover:text-gray-600">
            ✕ Cancel
          </button>
        )}
      </div>

      {isEdit ? (
        <>
          {/* Current show info */}
          <div className="bg-gray-50 rounded-xl p-4 mb-5 text-sm">
            <div className="flex items-start gap-3 mb-3">
              {selected.cover_art_url && (
                <img src={selected.cover_art_url} alt={selected.podcast_title}
                  className="w-12 h-12 rounded-lg flex-shrink-0 object-cover" />
              )}
              <div className="min-w-0">
                <p className="font-semibold text-gray-900">{selected.podcast_title}</p>
                <p className="text-xs text-gray-500">apple id: {selected.apple_podcast_id}</p>
              </div>
            </div>
            <div className="grid grid-cols-4 gap-2 text-xs text-gray-600">
              <div className="bg-white rounded-lg p-2 text-center">
                <p className="text-lg font-bold text-gray-900">{selected.episode_count}</p>
                <p className="text-gray-400">eps</p>
              </div>
              <div className="bg-white rounded-lg p-2 text-center">
                <p className="text-lg font-bold text-gray-900">{selected.host_count}</p>
                <p className="text-gray-400">hosts</p>
              </div>
              <div className="bg-white rounded-lg p-2 text-center">
                <p className="text-lg font-bold text-gray-900">{selected.guest_count}</p>
                <p className="text-gray-400">guests</p>
              </div>
              <div className="bg-white rounded-lg p-2 text-center flex flex-col items-center justify-center">
                <span className={`px-2 py-0.5 rounded-full font-medium ${(STATUS_BADGE[selected.status] || STATUS_BADGE.pending).bg}`}>
                  {(STATUS_BADGE[selected.status] || STATUS_BADGE.pending).label}
                </span>
                <p className="text-gray-400 mt-1">status</p>
              </div>
            </div>
            {selected.earliest_episode_date && (
              <p className="text-xs text-gray-500 mt-2">
                Earliest episode: {formatDateOnly(selected.earliest_episode_date)}
              </p>
            )}
            {selected.latest_episode_date && (
              <p className="text-xs text-gray-500">
                Latest episode: {formatDateOnly(selected.latest_episode_date)}
              </p>
            )}
            {selected.last_scraped_at && (
              <p className="text-xs text-gray-500">
                Last scraped: {new Date(selected.last_scraped_at).toLocaleString()}
              </p>
            )}
            {selected.error_message && (
              <p className="text-xs text-red-500 mt-2">{selected.error_message}</p>
            )}
          </div>

          {/* Show hosts */}
          <div className="mb-5">
            <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
              Hosts ({hosts.length})
            </p>
            {hosts.length === 0 ? (
              <p className="text-sm text-gray-400 italic mb-2">No hosts yet</p>
            ) : (
              <div className="space-y-1.5 mb-2">
                {hosts.map(h => {
                  const badge = SOURCE_BADGE[h.data_source] || SOURCE_BADGE.manual;
                  return (
                    <div key={h.host_id} className="flex items-center gap-2 py-1.5 px-3 bg-gray-50 rounded-lg text-sm">
                      {h.profile_image_url ? (
                        <img src={h.profile_image_url} alt={h.name}
                          className="w-6 h-6 rounded-full object-cover flex-shrink-0"
                          onError={e => { e.target.style.display = 'none'; }} />
                      ) : (
                        <span className="w-2 h-2 rounded-full bg-green-500 flex-shrink-0" />
                      )}
                      <a href={`/admin/people?host_id=${h.host_id}`}
                        className="font-medium text-gray-800 hover:text-teal-600 flex-1 truncate">
                        {h.name}
                      </a>
                      <span className={`text-xs px-1.5 py-0.5 rounded flex-shrink-0 ${badge.bg}`}>{badge.label}</span>
                      <button
                        onClick={() => handleRemoveShowHost(h.host_id)}
                        disabled={removingHostId === h.host_id}
                        className="text-gray-300 hover:text-red-500 disabled:opacity-40 flex-shrink-0 text-xs px-1"
                        title="Unlink"
                      >
                        {removingHostId === h.host_id ? '…' : '✕'}
                      </button>
                    </div>
                  );
                })}
              </div>
            )}
            <input
              type="text"
              value={hostQ}
              onChange={e => setHostQ(e.target.value)}
              placeholder="Search people to add as a host..."
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-teal-500 focus:outline-none"
            />
            {hostResults.length > 0 && (
              <div className="mt-2 border border-gray-100 rounded-lg divide-y divide-gray-50 overflow-hidden">
                {hostResults.map(p => (
                  <div key={p.host_id} className="flex items-center gap-2 px-3 py-2 text-sm">
                    <span className="flex-1 truncate text-gray-800">{p.full_name}</span>
                    <button
                      onClick={() => handleAddShowHost(p.host_id)}
                      disabled={addingHostId === p.host_id}
                      className="text-xs px-2 py-1 rounded bg-green-50 text-green-700 hover:bg-green-100 disabled:opacity-40"
                    >
                      + Host
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>

          <button onClick={handleScrapeNow} disabled={submitting}
            className="w-full py-2.5 bg-teal-600 hover:bg-teal-700 disabled:opacity-40 text-white font-semibold rounded-xl text-sm transition-colors">
            {submitting ? 'Starting…' : '🔄 Scrape Now'}
          </button>

          {/* Recent episodes */}
          {episodes.length > 0 && (
            <div className="mt-5 border-t border-gray-100 pt-4">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-3">
                Episodes ({episodesTotal})
              </p>
              <div className="space-y-1.5">
                {episodes.map(ep => (
                  <a key={ep.episode_id} href={`/admin/episodes?episode_id=${ep.episode_id}`}
                    className="block rounded-lg bg-gray-50 hover:bg-gray-100 px-3 py-2 transition-colors">
                    <p className="text-xs text-gray-700 leading-snug truncate">{ep.title}</p>
                    <p className="text-xs text-gray-400">
                      {ep.published_date && formatDateOnly(ep.published_date)}
                      {' · '}{ep.credit_count} credit{ep.credit_count !== 1 ? 's' : ''}
                    </p>
                  </a>
                ))}
              </div>
              {episodesTotal > episodes.length && (
                <a href={`/admin/episodes?show=${encodeURIComponent(selected.podcast_title)}`}
                  className="block text-center text-xs text-teal-600 hover:text-teal-700 mt-3">
                  View all {episodesTotal} episodes in Episode Admin →
                </a>
              )}
            </div>
          )}
        </>
      ) : (
        <>
          <div className="mb-4">
            <label className="block text-xs font-medium text-gray-500 mb-1">Apple Podcasts URL or ID</label>
            <input
              type="text"
              value={newShowInput}
              onChange={e => setNewShowInput(e.target.value)}
              placeholder="https://podcasts.apple.com/... or 1234567890"
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-teal-500 focus:outline-none"
            />
          </div>
          <button onClick={handleAdd} disabled={submitting}
            className="w-full py-2.5 bg-green-600 hover:bg-green-700 disabled:opacity-40 text-white font-semibold rounded-xl text-sm transition-colors">
            {submitting ? 'Working...' : '✅ Add + Scrape Now'}
          </button>
        </>
      )}

      {error && <p className="text-sm text-red-600 mt-3">{error}</p>}

      {result && (
        <div className="mt-4 p-4 bg-green-50 border border-green-200 rounded-xl text-sm">
          <p className="font-semibold text-green-800">✅ {isEdit ? 'Scrape started for' : 'Added'}: {result.name}</p>
          <p className="text-green-700 text-xs mt-0.5">
            {result.scraping
              ? 'Scrape started — check back in a few minutes'
              : result.message}
          </p>
        </div>
      )}
    </div>
  );
}

// ─── Main page ─────────────────────────────────────────────────────────────────
export default function AdminShows() {
  const [shows, setShows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [listError, setListError] = useState(null);
  const [searchQ, setSearchQ] = useState('');
  const [filter, setFilter] = useState('all');
  const [sort, setSort] = useState('title_asc');
  const [selected, setSelected] = useState(null);
  const searchRef = useRef(null);

  const fetchShows = useCallback(async () => {
    setLoading(true);
    setListError(null);
    try {
      const res = await adminFetch(`${API}/shows`);
      if (!res.ok) throw new Error(`API error ${res.status}`);
      setShows(await res.json());
    } catch (e) {
      setListError(e.message || 'Failed to load shows');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchShows(); }, [fetchShows]);

  // Deep-link support: /admin/shows?apple_podcast_id=X auto-opens that show
  useEffect(() => {
    const targetId = new URLSearchParams(window.location.search).get('apple_podcast_id');
    if (!targetId || shows.length === 0) return;
    const match = shows.find(s => s.apple_podcast_id === targetId);
    if (match) setSelected(match);
  }, [shows]);

  const visibleShows = useMemo(() => {
    let items = shows;
    if (filter !== 'all') items = items.filter(s => s.status === filter);
    if (searchQ.trim()) {
      const q = searchQ.trim().toLowerCase();
      items = items.filter(s => s.podcast_title?.toLowerCase().includes(q));
    }
    items = [...items];
    if (sort === 'title_asc') items.sort((a, b) => (a.podcast_title || '').localeCompare(b.podcast_title || ''));
    else if (sort === 'episodes_desc') items.sort((a, b) => b.episode_count - a.episode_count);
    else if (sort === 'guests_desc') items.sort((a, b) => b.guest_count - a.guest_count);
    else if (sort === 'guests_asc') items.sort((a, b) => a.guest_count - b.guest_count);
    else if (sort === 'recent_scrape') items.sort((a, b) => new Date(b.last_scraped_at || 0) - new Date(a.last_scraped_at || 0));
    return items;
  }, [shows, filter, searchQ, sort]);

  const handleDone = () => {
    fetchShows();
  };

  return (
    <div className="min-h-screen bg-gray-100 font-sans">
      <AdminHeader active="Shows" right={<span className="text-sm text-gray-400">{shows.length} shows</span>} />

      <div className="flex gap-6 p-6 max-w-7xl mx-auto">

        {/* LEFT — shows list */}
        <div className="flex-1 min-w-0">

          {/* Search + filters */}
          <div className="bg-white rounded-2xl border border-gray-200 p-4 mb-4">
            <div className="relative mb-3">
              <input ref={searchRef} type="text" value={searchQ} onChange={e => setSearchQ(e.target.value)}
                placeholder="Search by name..."
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-teal-500 focus:outline-none pr-7" />
              {searchQ && (
                <button
                  onClick={() => setSearchQ('')}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600 text-lg leading-none"
                >×</button>
              )}
            </div>

            <div className="flex gap-2 flex-wrap mb-3">
              {FILTERS.map(f => (
                <button key={f.id} onClick={() => setFilter(f.id)}
                  className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                    filter === f.id ? 'bg-teal-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}>
                  {f.label}
                </button>
              ))}
            </div>

            <div className="flex items-center gap-2">
              <span className="text-xs text-gray-400">Sort:</span>
              <select value={sort} onChange={e => setSort(e.target.value)}
                className="text-xs border border-gray-200 rounded px-2 py-1 focus:outline-none focus:border-teal-400">
                {SORTS.map(s => <option key={s.id} value={s.id}>{s.label}</option>)}
              </select>
            </div>
          </div>

          {/* Shows list */}
          <div className="bg-white rounded-2xl border border-gray-200 overflow-hidden" style={{ maxHeight: "calc(100vh - 280px)", overflowY: "auto" }}>
            {loading ? (
              <div className="py-12 text-center text-gray-400 text-sm">Loading...</div>
            ) : listError ? (
              <div className="py-12 flex flex-col items-center gap-3">
                <p className="text-sm font-semibold text-red-500">Couldn't load shows</p>
                <p className="text-xs text-gray-500">{listError}</p>
                <button
                  className="px-3 py-1.5 bg-teal-600 text-white text-sm rounded hover:bg-teal-700"
                  onClick={fetchShows}
                >
                  Retry
                </button>
              </div>
            ) : visibleShows.length === 0 ? (
              <div className="py-12 text-center text-gray-400 text-sm">No shows found</div>
            ) : (
              <div className="divide-y divide-gray-100">
                {visibleShows.map(show => {
                  const badge = STATUS_BADGE[show.status] || STATUS_BADGE.pending;
                  const isSelected = selected?.apple_podcast_id === show.apple_podcast_id;
                  return (
                    <div key={show.apple_podcast_id}
                      className={`flex items-center gap-3 px-4 py-3 hover:bg-gray-50 cursor-pointer transition-colors ${
                        isSelected ? 'bg-teal-50 border-l-2 border-teal-500' : ''
                      }`}
                      onClick={() => setSelected(isSelected ? null : show)}>
                      {show.cover_art_url && (
                        <img src={show.cover_art_url} alt={show.podcast_title}
                          className="w-8 h-8 rounded object-cover flex-shrink-0" />
                      )}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <p className="text-sm font-medium text-gray-900 truncate">{show.podcast_title}</p>
                          <span className={`text-xs px-1.5 py-0.5 rounded flex-shrink-0 ${badge.bg}`}>
                            {badge.label}
                          </span>
                        </div>
                        <p className="text-xs text-gray-400">
                          {show.episode_count} episode{show.episode_count !== 1 ? 's' : ''}
                          {' · '}{show.host_count} host{show.host_count !== 1 ? 's' : ''}
                          {' · '}{show.guest_count} guest{show.guest_count !== 1 ? 's' : ''}
                          {show.latest_episode_date && ` · latest ${formatDateOnly(show.latest_episode_date)}`}
                          {show.last_scraped_at && ` · scraped ${new Date(show.last_scraped_at).toLocaleDateString()}`}
                        </p>
                        {show.error_message && (
                          <p className="text-xs text-red-500 truncate">{show.error_message}</p>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>

        {/* RIGHT — add/edit panel */}
        <div className="w-96 flex-shrink-0">
          <ShowPanel
            selected={selected}
            onDone={handleDone}
            onCancel={() => setSelected(null)}
          />
        </div>
      </div>
    </div>
  );
}
