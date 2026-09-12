import { useState, useEffect, useCallback, useRef } from 'react';
import { API_BASE_URL } from '../config';
import { adminFetch } from '../adminAuth';
import AdminHeader from './AdminHeader';

const API = `${API_BASE_URL}/api/admin`;

export default function AdminImages() {
  const [person, setPerson] = useState(null);
  const [stats, setStats] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [twitterUrl, setTwitterUrl] = useState('');
  const [preview, setPreview] = useState(null);
  const [submitting, setSubmitting] = useState(false);
  const [lastResult, setLastResult] = useState(null);
  const [done, setDone] = useState(false);
  const [skippedIds, setSkippedIds] = useState(new Set());
  const skippedIdsRef = useRef(new Set());

  const fetchNext = useCallback(async (clearResult = true) => {
    setLoading(true);
    setError(null);
    setTwitterUrl('');
    setPreview(null);
    if (clearResult) setLastResult(null);
    try {
      const skipped = Array.from(skippedIdsRef.current).join(',');
      const res = await adminFetch(`${API}/images/next${skipped ? '?skip=' + skipped : ''}`);
      if (!res.ok) throw new Error(`API error ${res.status}`);
      const data = await res.json();
      if (data.done) { setDone(true); setPerson(null); }
      else { setPerson(data); setDone(false); }
    } catch (e) { setError(e.message || 'Failed to load'); }
    finally { setLoading(false); }
  }, []);

  const fetchStats = useCallback(async () => {
    try {
      const res = await adminFetch(`${API}/images/stats`);
      setStats(await res.json());
    } catch (e) {}
  }, []);

  useEffect(() => { fetchNext(); fetchStats(); }, [fetchNext, fetchStats]);

  const handleSubmitUrl = async () => {
    if (!twitterUrl.trim() || !person || submitting) return;
    setSubmitting(true);
    try {
      const res = await adminFetch(`${API}/images/${person.host_id}/set_twitter`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ twitter_url: twitterUrl.trim() }),
      });
      const data = await res.json();
      if (data.success) setPreview(data);
      else alert(data.detail || 'Could not extract handle from URL');
    } catch (e) { console.error(e); }
    finally { setSubmitting(false); }
  };

  const handleApprove = async () => {
    if (!person || !preview) return;
    setSubmitting(true);
    try {
      const res = await adminFetch(`${API}/images/${person.host_id}/approve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ image_url: preview.image_url }),
      });
      const data = await res.json();
      setLastResult({ action: 'approve', ...data });
      await fetchStats();
      await fetchNext(false);
    } catch (e) { console.error(e); }
    finally { setSubmitting(false); }
  };

  const handleSkip = () => {
    if (!person) return;
    skippedIdsRef.current = new Set([...skippedIdsRef.current, person.host_id]);
    setSkippedIds(new Set(skippedIdsRef.current));
    setLastResult({ action: 'skip' });
    fetchNext(false);
  };

  return (
    <div className="min-h-screen bg-gray-100 font-sans">
      <AdminHeader
        active="Images"
        right={
          stats.total && (
            <div className="flex items-center gap-4 text-sm text-gray-500">
              <span className="text-green-600 font-medium">{stats.with_image} with image</span>
              <span className="text-orange-600 font-medium">{stats.missing} missing</span>
              <span className="text-gray-400">({Math.round(stats.with_image / stats.total * 100)}% coverage)</span>
            </div>
          )
        }
      />

      {loading && <div className="flex items-center justify-center h-96 text-gray-400">Loading...</div>}

      {error && !loading && (
        <div className="flex flex-col items-center justify-center h-96 gap-3">
          <p className="text-lg font-semibold text-red-500">Couldn't load</p>
          <p className="text-sm text-gray-500">{error}</p>
          <button
            className="px-4 py-2 bg-teal-600 text-white rounded hover:bg-teal-700"
            onClick={() => fetchNext()}
          >
            Retry
          </button>
        </div>
      )}

      {done && !loading && !error && (
        <div className="flex flex-col items-center justify-center h-96 gap-3">
          <div className="text-4xl">🎉</div>
          <p className="text-xl font-semibold text-gray-700">Everyone has a profile image!</p>
        </div>
      )}

      {person && !loading && !error && (
        <div className="max-w-xl mx-auto py-8 px-6">

          <div className="text-center mb-6">
            <h2 className="text-3xl font-bold text-gray-900 mb-1">
              <a href={`/admin/people?host_id=${person.host_id}`} className="hover:text-teal-600">
                {person.host_name}
              </a>
            </h2>
            <p className="text-gray-500 text-sm">
              {person.appearances} appearance{person.appearances !== 1 ? 's' : ''}
              {person.podcasts?.length > 0 && <span> · {person.podcasts.slice(0, 3).join(', ')}{person.podcasts.length > 3 ? '...' : ''}</span>}
            </p>
            <p className="text-xs text-gray-400 mt-1">{person.total_missing} people still need images</p>
          </div>

          {preview ? (
            <div className="bg-white rounded-2xl shadow-sm border border-gray-200 overflow-hidden mb-4">
              <div className="bg-gray-50 flex items-center justify-center p-6">
                <img
                  src={preview.image_url}
                  alt={person.host_name}
                  className="w-40 h-40 rounded-full object-cover shadow"
                  onError={e => { e.target.style.display='none'; }}
                />
              </div>
              <div className="px-4 py-3 border-t border-gray-100 text-center">
                <p className="text-sm text-gray-600"><span className="font-medium">@{preview.handle}</span> on {preview.platform === 'bluesky' ? 'Bluesky' : 'Twitter/X'}</p>
                <p className="text-xs text-gray-400 mt-0.5">{preview.image_url}</p>
              </div>
            </div>
          ) : (
            <div className="bg-white rounded-2xl shadow-sm border border-gray-200 flex items-center justify-center mb-4" style={{ height: 200 }}>
              <div className="text-center text-gray-300">
                <p className="text-5xl mb-2">👤</p>
                <p className="text-sm">Paste their Twitter/X URL below</p>
              </div>
            </div>
          )}

          <div className="flex gap-2 mb-4">
            <input
              type="text"
              value={twitterUrl}
              onChange={e => { setTwitterUrl(e.target.value); setPreview(null); }}
              onKeyDown={e => e.key === 'Enter' && handleSubmitUrl()}
              placeholder="https://x.com/handle or https://bsky.app/profile/handle"
              className="flex-1 rounded-xl border border-gray-300 px-4 py-3 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
            />
            <a
              href={`https://x.com/search?q=${encodeURIComponent(person?.host_name || '')}&src=typed_query&f=user`}
              target="_blank" rel="noopener noreferrer"
              className="px-3 py-3 bg-gray-50 hover:bg-gray-100 border border-gray-200 rounded-xl text-gray-700 text-sm font-bold transition-colors flex-shrink-0"
              title="Search X"
            >𝕏</a>
            <a
              href={`https://bsky.app/search?q=${encodeURIComponent(person?.host_name || '')}`}
              target="_blank" rel="noopener noreferrer"
              className="px-3 py-3 bg-sky-50 hover:bg-sky-100 border border-sky-200 rounded-xl text-sky-600 text-sm transition-colors flex-shrink-0"
              title="Search Bluesky"
            >🦋</a>
            <button
              onClick={handleSubmitUrl}
              disabled={!twitterUrl.trim() || submitting}
              className="px-4 py-3 bg-blue-600 hover:bg-blue-700 disabled:opacity-40 text-white font-medium rounded-xl text-sm transition-colors"
            >
              Preview
            </button>
          </div>

          <div className="space-y-2">
            <button
              onClick={handleApprove}
              disabled={!preview || submitting}
              className="w-full py-3 px-6 bg-green-600 hover:bg-green-700 disabled:opacity-40 text-white font-semibold rounded-xl transition-colors"
            >
              ✅ Approve Image
            </button>
            <button
              onClick={handleSkip}
              disabled={submitting}
              className="w-full py-3 px-6 bg-gray-200 hover:bg-gray-300 disabled:opacity-40 text-gray-700 font-semibold rounded-xl transition-colors"
            >
              ⏭ Skip
            </button>
          </div>

          {lastResult && (
            <div className={`mt-4 p-3 rounded-xl text-sm text-center ${
              lastResult.action === 'approve'
                ? 'bg-green-50 border border-green-200 text-green-800'
                : 'bg-gray-100 text-gray-600'
            }`}>
              {lastResult.action === 'approve' ? `✅ Saved image for ${lastResult.name}` : '⏭ Skipped'}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
