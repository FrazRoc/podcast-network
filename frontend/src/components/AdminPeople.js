import { useState, useEffect, useCallback } from 'react';

const API = 'http://localhost:8000/api/admin';

const PROXY = (url) =>
  url && !url.includes('mzstatic.com') && !url.includes('cdn.bsky.app')
    ? `http://localhost:8000/api/proxy/image?url=${encodeURIComponent(url)}`
    : url;

export default function AdminPeople() {
  const [form, setForm]         = useState({ first_name: '', last_name: '', twitter_url: '', bluesky_url: '' });
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult]     = useState(null);
  const [error, setError]       = useState('');
  const [searchQ, setSearchQ]   = useState('');
  const [people, setPeople]     = useState([]);
  const [loadingPeople, setLoadingPeople] = useState(false);
  const [deleteConfirm, setDeleteConfirm] = useState(null);

  const fetchPeople = useCallback(async (q = '') => {
    setLoadingPeople(true);
    try {
      const res = await fetch(`${API}/people?q=${encodeURIComponent(q)}`);
      setPeople(await res.json());
    } catch (e) { console.error(e); }
    finally { setLoadingPeople(false); }
  }, []);

  useEffect(() => { fetchPeople(); }, [fetchPeople]);

  const handleSearch = (e) => {
    const q = e.target.value;
    setSearchQ(q);
    fetchPeople(q);
  };

  const [existingHostId, setExistingHostId] = useState(null);

  const handleSubmit = async () => {
    if (!form.first_name.trim() || !form.last_name.trim()) {
      setError('First name and last name are required');
      return;
    }
    setSubmitting(true);
    setError('');
    setResult(null);
    setExistingHostId(null);
    try {
      const res = await fetch(`${API}/people`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(form),
      });
      const data = await res.json();
      if (res.status === 409) {
        // Person already exists — extract host_id and offer to scan
        const match = data.detail?.match(/host_id=(\d+)/);
        setExistingHostId(match ? parseInt(match[1]) : null);
        setError(data.detail);
      } else if (!res.ok) {
        setError(data.detail || 'Error creating person');
      } else {
        setResult(data);
        setForm({ first_name: '', last_name: '', twitter_url: '', bluesky_url: '' });
        fetchPeople(searchQ);
      }
    } catch (e) {
      setError(e.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleScanExisting = async () => {
    if (!existingHostId) return;
    setSubmitting(true);
    setError('');
    try {
      const res = await fetch(`${API}/people/${existingHostId}/scan`, { method: 'POST' });
      const data = await res.json();
      setResult(data);
      setExistingHostId(null);
      fetchPeople(searchQ);
    } catch (e) {
      setError(e.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (host_id, name) => {
    if (deleteConfirm !== host_id) {
      setDeleteConfirm(host_id);
      return;
    }
    try {
      await fetch(`${API}/people/${host_id}`, { method: 'DELETE' });
      setDeleteConfirm(null);
      fetchPeople(searchQ);
    } catch (e) { console.error(e); }
  };

  return (
    <div className="min-h-screen bg-gray-100 font-sans">

      {/* Header */}
      <header className="bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <a href="/" className="text-gray-400 hover:text-gray-600 text-sm">← Network</a>
          <a href="/admin" className="text-gray-400 hover:text-gray-600 text-sm">Suggestions</a>
          <a href="/admin/images" className="text-gray-400 hover:text-gray-600 text-sm">Images</a>
          <h1 className="text-lg font-semibold text-gray-900">People</h1>
        </div>
      </header>

      <div className="max-w-3xl mx-auto py-8 px-6 space-y-8">

        {/* Add Person Form */}
        <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Add New Person</h2>

          <div className="grid grid-cols-2 gap-4 mb-4">
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">First Name *</label>
              <input
                type="text"
                value={form.first_name}
                onChange={e => setForm(f => ({ ...f, first_name: e.target.value }))}
                placeholder="Shayle"
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Last Name *</label>
              <input
                type="text"
                value={form.last_name}
                onChange={e => setForm(f => ({ ...f, last_name: e.target.value }))}
                placeholder="Kann"
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
              />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4 mb-4">
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Twitter/X URL</label>
              <input
                type="text"
                value={form.twitter_url}
                onChange={e => setForm(f => ({ ...f, twitter_url: e.target.value }))}
                placeholder="https://x.com/shaylekann"
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Bluesky URL</label>
              <input
                type="text"
                value={form.bluesky_url}
                onChange={e => setForm(f => ({ ...f, bluesky_url: e.target.value }))}
                placeholder="https://bsky.app/profile/handle.bsky.social"
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
              />
            </div>
          </div>

          {error && (
            <div className="mb-3">
              <p className="text-sm text-red-600">{error}</p>
              {existingHostId && (
                <button
                  onClick={handleScanExisting}
                  disabled={submitting}
                  className="mt-2 px-4 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:opacity-40 text-white text-sm font-medium rounded-lg transition-colors"
                >
                  🔍 Scan Episodes for This Person
                </button>
              )}
            </div>
          )}

          <button
            onClick={handleSubmit}
            disabled={submitting}
            className="w-full py-2.5 bg-green-600 hover:bg-green-700 disabled:opacity-40 text-white font-semibold rounded-xl text-sm transition-colors"
          >
            {submitting ? 'Creating...' : '✅ Create Person + Scan Episodes'}
          </button>

          {/* Result */}
          {result && (
            <div className="mt-4 p-4 bg-green-50 border border-green-200 rounded-xl text-sm">
              <div className="flex items-center gap-3 mb-2">
                {result.image_url && (
                  <img src={PROXY(result.image_url)} alt={result.name}
                    className="w-10 h-10 rounded-full object-cover" />
                )}
                <div>
                  <p className="font-semibold text-green-800">✅ Created: {result.name}</p>
                  <p className="text-green-700">
                    {result.episodes_linked > 0
                      ? `Found ${result.episodes_linked} episode appearance(s)`
                      : 'No episode appearances found'}
                  </p>
                </div>
              </div>
              {result.by_podcast && Object.keys(result.by_podcast).length > 0 && (
                <ul className="mt-2 space-y-0.5 text-green-700">
                  {Object.entries(result.by_podcast).map(([show, count]) => (
                    <li key={show}>• {show} ({count} episode{count !== 1 ? 's' : ''})</li>
                  ))}
                </ul>
              )}
            </div>
          )}
        </div>

        {/* People List */}
        <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-gray-900">People</h2>
            <input
              type="text"
              value={searchQ}
              onChange={handleSearch}
              placeholder="Search by name..."
              className="rounded-lg border border-gray-300 px-3 py-1.5 text-sm w-56 focus:border-blue-500 focus:outline-none"
            />
          </div>

          {loadingPeople ? (
            <p className="text-sm text-gray-400 text-center py-4">Loading...</p>
          ) : (
            <div className="space-y-2">
              {people.map(person => (
                <div key={person.host_id} className="flex items-center gap-3 py-2 px-3 rounded-lg hover:bg-gray-50">
                  {person.profile_image_url ? (
                    <img src={PROXY(person.profile_image_url)} alt={person.full_name}
                      className="w-8 h-8 rounded-full object-cover flex-shrink-0"
                      onError={e => { e.target.style.display = 'none'; }} />
                  ) : (
                    <div className="w-8 h-8 rounded-full bg-gray-200 flex-shrink-0 flex items-center justify-center text-xs font-medium text-gray-500">
                      {person.first_name?.[0]}{person.last_name?.[0]}
                    </div>
                  )}
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-gray-900">{person.full_name}</p>
                    <p className="text-xs text-gray-400">
                      {person.appearances} episode{person.appearances !== 1 ? 's' : ''}
                      {person.podcast_count > 0 && ` · ${person.podcast_count} show${person.podcast_count !== 1 ? 's' : ''}`}
                      {person.data_source && ` · ${person.data_source}`}
                    </p>
                  </div>
                  <button
                    onClick={() => handleDelete(person.host_id, person.full_name)}
                    className={`text-xs px-2 py-1 rounded transition-colors flex-shrink-0 ${
                      deleteConfirm === person.host_id
                        ? 'bg-red-600 text-white'
                        : 'text-gray-400 hover:text-red-600 hover:bg-red-50'
                    }`}
                  >
                    {deleteConfirm === person.host_id ? 'Confirm?' : 'Delete'}
                  </button>
                </div>
              ))}
              {people.length === 0 && (
                <p className="text-sm text-gray-400 text-center py-4">No people found</p>
              )}
              {people.length === 50 && (
                <p className="text-xs text-gray-400 text-center pt-2">Showing top 50 — search to narrow down</p>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
