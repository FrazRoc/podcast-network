import { useState, useEffect, useCallback, useRef } from 'react';

const API = 'http://localhost:8000/api/admin';

const PROXY = (url) =>
  url && !url.includes('mzstatic.com') && !url.includes('cdn.bsky.app')
    ? `http://localhost:8000/api/proxy/image?url=${encodeURIComponent(url)}`
    : url;

const SOURCE_BADGE = {
  apple_verified:     { label: 'Apple',    bg: 'bg-green-100 text-green-700' },
  approved_suggestion:{ label: 'Approved', bg: 'bg-blue-100 text-blue-700' },
  itunes_artist:      { label: 'iTunes',   bg: 'bg-purple-100 text-purple-700' },
  parsed_desc:        { label: 'Parsed',   bg: 'bg-yellow-100 text-yellow-700' },
  parsed_title:       { label: 'Parsed',   bg: 'bg-yellow-100 text-yellow-700' },
  manual:             { label: 'Manual',   bg: 'bg-gray-100 text-gray-600' },
};

const FILTERS = [
  { id: 'all',      label: 'All' },
  { id: 'zero',     label: '0 appearances' },
  { id: 'parsed',   label: 'Parsed only' },
  { id: 'no_image', label: 'No image' },
];

const SORTS = [
  { id: 'appearances_desc', label: 'Most appearances' },
  { id: 'appearances_asc',  label: 'Fewest appearances' },
  { id: 'name_asc',         label: 'Name A–Z' },
  { id: 'name_desc',        label: 'Name Z–A' },
  { id: 'newest',           label: 'Newest first' },
];

const emptyForm = { first_name: '', last_name: '', twitter_url: '', bluesky_url: '' };

// ─── Right panel: Add or Edit ──────────────────────────────────────────────────
function PersonPanel({ selected, onSaved, onCancel }) {
  const [form, setForm]         = useState(emptyForm);
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult]     = useState(null);
  const [error, setError]       = useState('');
  const [existingId, setExistingId] = useState(null);
  const [episodes, setEpisodes] = useState([]);
  const [expandedShows, setExpandedShows] = useState({});
  const isEdit = !!selected;

  useEffect(() => {
    if (!selected) { setEpisodes([]); return; }
    fetch(`${API}/people/${selected.host_id}/episodes`)
      .then(r => r.json())
      .then(setEpisodes)
      .catch(console.error);
  }, [selected?.host_id]);

  const toggleShow = (show) =>
    setExpandedShows(prev => ({ ...prev, [show]: !prev[show] }));

  useEffect(() => {
    if (selected) {
      setForm({
        first_name:   selected.first_name  || '',
        last_name:    selected.last_name   || '',
        twitter_url:  selected.twitter_handle ? `https://x.com/${selected.twitter_handle}` : '',
        bluesky_url:  selected.bluesky_handle  ? `https://bsky.app/profile/${selected.bluesky_handle}` : '',
      });
      setResult(null);
      setError('');
      setExistingId(null);
    } else {
      setForm(emptyForm);
      setResult(null);
      setError('');
      setExistingId(null);
    }
  }, [selected]);

  const handleSubmit = async () => {
    if (!form.first_name.trim() || !form.last_name.trim()) {
      setError('First name and last name are required');
      return;
    }
    setSubmitting(true);
    setError('');
    setResult(null);
    setExistingId(null);

    try {
      let res, data;
      if (isEdit) {
        res  = await fetch(`${API}/people/${selected.host_id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(form),
        });
      } else {
        res = await fetch(`${API}/people`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(form),
        });
      }
      data = await res.json();

      if (res.status === 409) {
        const match = data.detail?.match(/host_id=(\d+)/);
        setExistingId(match ? parseInt(match[1]) : null);
        setError(data.detail);
      } else if (!res.ok) {
        setError(data.detail || 'Error');
      } else {
        setResult(data);
        if (!isEdit) setForm(emptyForm);
        onSaved?.();
      }
    } catch (e) {
      setError(e.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleScan = async (hostId) => {
    setSubmitting(true);
    setError('');
    try {
      const res  = await fetch(`${API}/people/${hostId}/scan`, { method: 'POST' });
      const data = await res.json();
      setResult(data);
      setExistingId(null);
      onSaved?.();
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
          {isEdit ? 'Edit Person' : 'Add New Person'}
        </h2>
        {isEdit && (
          <button onClick={onCancel} className="text-xs text-gray-400 hover:text-gray-600">
            ✕ Cancel
          </button>
        )}
      </div>

      {/* Current person info */}
      {isEdit && (
        <div className="bg-gray-50 rounded-xl p-4 mb-5 text-sm">
          <div className="flex items-center gap-3 mb-3">
            {selected.profile_image_url ? (
              <img src={PROXY(selected.profile_image_url)} alt={selected.full_name}
                className="w-12 h-12 rounded-full object-cover flex-shrink-0" />
            ) : (
              <div className="w-12 h-12 rounded-full bg-gray-200 flex-shrink-0 flex items-center justify-center text-sm font-medium text-gray-500">
                {selected.first_name?.[0]}{selected.last_name?.[0]}
              </div>
            )}
            <div>
              <p className="font-semibold text-gray-900">{selected.full_name}</p>
              <p className="text-xs text-gray-500">host_id: {selected.host_id} · {selected.data_source}</p>
            </div>
          </div>
          <div className="grid grid-cols-2 gap-2 text-xs text-gray-600">
            <div className="bg-white rounded-lg p-2 text-center">
              <p className="text-lg font-bold text-gray-900">{selected.appearances}</p>
              <p className="text-gray-400">episodes</p>
            </div>
            <div className="bg-white rounded-lg p-2 text-center">
              <p className="text-lg font-bold text-gray-900">{selected.podcast_count}</p>
              <p className="text-gray-400">shows</p>
            </div>
          </div>
          {(selected.twitter_handle || selected.bluesky_handle) && (
            <div className="mt-2 flex gap-2 flex-wrap">
              {selected.twitter_handle && (
                <a href={`https://x.com/${selected.twitter_handle}`} target="_blank" rel="noopener noreferrer"
                  className="text-xs text-blue-600 hover:underline">@{selected.twitter_handle}</a>
              )}
              {selected.bluesky_handle && (
                <a href={`https://bsky.app/profile/${selected.bluesky_handle}`} target="_blank" rel="noopener noreferrer"
                  className="text-xs text-blue-600 hover:underline">{selected.bluesky_handle}</a>
              )}
            </div>
          )}
        </div>
      )}

      <div className="space-y-3 mb-4">
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">First Name *</label>
            <input type="text" value={form.first_name}
              onChange={e => setForm(f => ({ ...f, first_name: e.target.value }))}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Last Name *</label>
            <input type="text" value={form.last_name}
              onChange={e => setForm(f => ({ ...f, last_name: e.target.value }))}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
          </div>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">Twitter/X URL</label>
          <input type="text" value={form.twitter_url}
            onChange={e => setForm(f => ({ ...f, twitter_url: e.target.value }))}
            placeholder="https://x.com/handle"
            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">Bluesky URL</label>
          <input type="text" value={form.bluesky_url}
            onChange={e => setForm(f => ({ ...f, bluesky_url: e.target.value }))}
            placeholder="https://bsky.app/profile/handle.bsky.social"
            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
        </div>
      </div>

      {error && (
        <div className="mb-3">
          <p className="text-sm text-red-600">{error}</p>
          {existingId && (
            <button onClick={() => handleScan(existingId)} disabled={submitting}
              className="mt-2 px-3 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:opacity-40 text-white text-xs font-medium rounded-lg">
              🔍 Scan Episodes for This Person
            </button>
          )}
        </div>
      )}

      <button onClick={handleSubmit} disabled={submitting}
        className="w-full py-2.5 bg-green-600 hover:bg-green-700 disabled:opacity-40 text-white font-semibold rounded-xl text-sm transition-colors">
        {submitting ? 'Working...' : isEdit ? '💾 Save + Re-scan Episodes' : '✅ Create + Scan Episodes'}
      </button>

      {result && (
        <div className="mt-4 p-4 bg-green-50 border border-green-200 rounded-xl text-sm">
          <div className="flex items-center gap-3 mb-1">
            {result.image_url && (
              <img src={PROXY(result.image_url)} alt={result.name}
                className="w-9 h-9 rounded-full object-cover flex-shrink-0" />
            )}
            <div>
              <p className="font-semibold text-green-800">
                {isEdit ? '✅ Updated:' : '✅ Created:'} {result.name}
              </p>
              <p className="text-green-700 text-xs">
                {result.episodes_linked > 0
                  ? `Linked ${result.episodes_linked} episode(s)`
                  : 'No new episode links found'}
              </p>
            </div>
          </div>
          {result.by_podcast && Object.keys(result.by_podcast).length > 0 && (
            <ul className="mt-2 space-y-0.5 text-green-700 text-xs">
              {Object.entries(result.by_podcast).map(([show, count]) => (
                <li key={show}>• {show} ({count})</li>
              ))}
            </ul>
          )}
        </div>
      )}
      {/* Episodes list */}
      {isEdit && episodes.length > 0 && (
        <div className="mt-5 border-t border-gray-100 pt-4">
          <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-3">
            Episode Appearances ({episodes.reduce((s, p) => s + p.count, 0)} total)
          </p>
          <div className="space-y-2">
            {episodes.map(pod => (
              <div key={pod.podcast} className="rounded-lg border border-gray-100 overflow-hidden">
                <button
                  onClick={() => toggleShow(pod.podcast)}
                  className="w-full flex items-center gap-2 px-3 py-2 bg-gray-50 hover:bg-gray-100 text-left transition-colors"
                >
                  {pod.cover_art_url && (
                    <img src={pod.cover_art_url} alt={pod.podcast}
                      className="w-6 h-6 rounded flex-shrink-0" />
                  )}
                  <span className="text-xs font-medium text-gray-700 flex-1 truncate">{pod.podcast}</span>
                  <span className="text-xs text-gray-400 flex-shrink-0">{pod.count} ep{pod.count !== 1 ? 's' : ''}</span>
                  <span className="text-gray-300 text-xs">{expandedShows[pod.podcast] ? '▲' : '▼'}</span>
                </button>
                {expandedShows[pod.podcast] && (
                  <div className="divide-y divide-gray-50">
                    {pod.episodes.map(ep => (
                      <div key={ep.episode_id} className="px-3 py-1.5 flex items-start gap-2">
                        <span className={`mt-0.5 w-1.5 h-1.5 rounded-full flex-shrink-0 ${ep.is_guest ? 'bg-blue-400' : 'bg-green-500'}`} />
                        <div className="min-w-0">
                          <p className="text-xs text-gray-700 leading-snug">{ep.episode_title}</p>
                          <p className="text-xs text-gray-400">{ep.published_date?.slice(0,10)} · {ep.data_source}</p>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Main page ─────────────────────────────────────────────────────────────────
export default function AdminPeople() {
  const [people, setPeople]         = useState([]);
  const [loading, setLoading]       = useState(false);
  const [searchQ, setSearchQ]       = useState('');
  const [filter, setFilter]         = useState('all');
  const [sort, setSort]             = useState('appearances_desc');
  const [selected, setSelected]     = useState(null);  // person being edited
  const [deleteConfirm, setDeleteConfirm] = useState(null);
  const [total, setTotal]           = useState(0);
  const searchRef = useRef(null);

  const fetchPeople = useCallback(async (q = '', f = 'all', s = 'appearances_desc') => {
    setLoading(true);
    try {
      const params = new URLSearchParams({ q, filter: f, sort: s });
      const res = await fetch(`${API}/people?${params}`);
      const data = await res.json();
      const items = Array.isArray(data) ? data : (data.items || []);
      setPeople(items);
      setTotal(Array.isArray(data) ? data.length : (data.total || items.length));
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { fetchPeople(searchQ, filter, sort); }, [fetchPeople]);

  const handleSearch = (e) => {
    const q = e.target.value;
    setSearchQ(q);
    fetchPeople(q, filter, sort);
  };

  const handleFilter = (f) => {
    setFilter(f);
    fetchPeople(searchQ, f, sort);
  };

  const handleSort = (s) => {
    setSort(s);
    fetchPeople(searchQ, filter, s);
  };

  const handleDelete = async (host_id) => {
    if (deleteConfirm !== host_id) { setDeleteConfirm(host_id); return; }
    try {
      await fetch(`${API}/people/${host_id}`, { method: 'DELETE' });
      setDeleteConfirm(null);
      if (selected?.host_id === host_id) setSelected(null);
      fetchPeople(searchQ, filter, sort);
    } catch (e) { console.error(e); }
  };

  const handleSaved = () => {
    fetchPeople(searchQ, filter, sort);
  };

  return (
    <div className="min-h-screen bg-gray-100 font-sans">
      <header className="bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <a href="/" className="text-gray-400 hover:text-gray-600 text-sm">← Network</a>
          <a href="/admin" className="text-gray-400 hover:text-gray-600 text-sm">Suggestions</a>
          <a href="/admin/images" className="text-gray-400 hover:text-gray-600 text-sm">Images</a>
          <h1 className="text-lg font-semibold text-gray-900">People</h1>
        </div>
        <span className="text-sm text-gray-400">{total} people</span>
      </header>

      <div className="flex gap-6 p-6 max-w-7xl mx-auto">

        {/* LEFT — people list */}
        <div className="flex-1 min-w-0">

          {/* Search + filters */}
          <div className="bg-white rounded-2xl border border-gray-200 p-4 mb-4">
            <input ref={searchRef} type="text" value={searchQ} onChange={handleSearch}
              placeholder="Search by name..."
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm mb-3 focus:border-blue-500 focus:outline-none" />

            <div className="flex gap-2 flex-wrap mb-3">
              {FILTERS.map(f => (
                <button key={f.id} onClick={() => handleFilter(f.id)}
                  className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                    filter === f.id ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}>
                  {f.label}
                </button>
              ))}
            </div>

            <div className="flex items-center gap-2">
              <span className="text-xs text-gray-400">Sort:</span>
              <select value={sort} onChange={e => handleSort(e.target.value)}
                className="text-xs border border-gray-200 rounded px-2 py-1 focus:outline-none focus:border-blue-400">
                {SORTS.map(s => <option key={s.id} value={s.id}>{s.label}</option>)}
              </select>
            </div>
          </div>

          {/* People list */}
          <div className="bg-white rounded-2xl border border-gray-200 overflow-hidden" style={{ maxHeight: "calc(100vh - 280px)", overflowY: "auto" }}>
            {loading ? (
              <div className="py-12 text-center text-gray-400 text-sm">Loading...</div>
            ) : people.length === 0 ? (
              <div className="py-12 text-center text-gray-400 text-sm">No people found</div>
            ) : (
              <div className="divide-y divide-gray-100">
                {people.map(person => {
                  const badge = SOURCE_BADGE[person.data_source] || SOURCE_BADGE.manual;
                  const isSelected = selected?.host_id === person.host_id;
                  return (
                    <div key={person.host_id}
                      className={`flex items-center gap-3 px-4 py-3 hover:bg-gray-50 cursor-pointer transition-colors ${
                        isSelected ? 'bg-blue-50 border-l-2 border-blue-500' : ''
                      }`}
                      onClick={() => setSelected(isSelected ? null : person)}>

                      {/* Avatar */}
                      {person.profile_image_url ? (
                        <img src={PROXY(person.profile_image_url)} alt={person.full_name}
                          className="w-8 h-8 rounded-full object-cover flex-shrink-0"
                          onError={e => { e.target.style.display = 'none'; }} />
                      ) : (
                        <div className="w-8 h-8 rounded-full bg-gray-200 flex-shrink-0 flex items-center justify-center text-xs font-medium text-gray-500">
                          {person.first_name?.[0]}{person.last_name?.[0]}
                        </div>
                      )}

                      {/* Info */}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <p className="text-sm font-medium text-gray-900 truncate">{person.full_name}</p>
                          <span className={`text-xs px-1.5 py-0.5 rounded flex-shrink-0 ${badge.bg}`}>
                            {badge.label}
                          </span>
                        </div>
                        <p className="text-xs text-gray-400">
                          {person.appearances} ep{person.appearances !== 1 ? 's' : ''}
                          {person.podcast_count > 0 && ` · ${person.podcast_count} show${person.podcast_count !== 1 ? 's' : ''}`}
                          {person.twitter_handle && ` · @${person.twitter_handle}`}
                          {person.bluesky_handle && ` · ${person.bluesky_handle}`}
                        </p>
                      </div>

                      {/* Delete */}
                      <button onClick={e => { e.stopPropagation(); handleDelete(person.host_id); }}
                        className={`text-xs px-2 py-1 rounded flex-shrink-0 transition-colors ${
                          deleteConfirm === person.host_id
                            ? 'bg-red-600 text-white'
                            : 'text-gray-300 hover:text-red-500 hover:bg-red-50'
                        }`}>
                        {deleteConfirm === person.host_id ? 'Sure?' : '🗑'}
                      </button>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>

        {/* RIGHT — add/edit panel */}
        <div className="w-96 flex-shrink-0">
          <PersonPanel
            selected={selected}
            onSaved={handleSaved}
            onCancel={() => setSelected(null)}
          />
        </div>
      </div>
    </div>
  );
}
