import { useState, useEffect, useCallback } from 'react';
import { API_BASE_URL } from '../config';
import { adminFetch } from '../adminAuth';

const API = `${API_BASE_URL}/api/admin`;

const STATUS_BADGE = {
  pending:     { label: 'Pending',     bg: 'bg-gray-100 text-gray-600' },
  in_progress: { label: 'In Progress', bg: 'bg-blue-100 text-blue-700' },
  success:     { label: 'Success',     bg: 'bg-green-100 text-green-700' },
  failed:      { label: 'Failed',      bg: 'bg-red-100 text-red-700' },
};

// Accepts a raw Apple Podcast ID or a full podcasts.apple.com URL and
// pulls out the numeric ID, e.g. .../id1234567890 -> 1234567890
const extractAppleId = (input) => {
  const trimmed = input.trim();
  const match = trimmed.match(/id(\d+)/) || trimmed.match(/^(\d+)$/);
  return match ? match[1] : trimmed;
};

export default function AdminShows() {
  const [shows, setShows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [newShowInput, setNewShowInput] = useState('');
  const [adding, setAdding] = useState(false);
  const [addResult, setAddResult] = useState(null);
  const [scrapingId, setScrapingId] = useState(null);
  const [scrapeResults, setScrapeResults] = useState({});

  const fetchShows = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await adminFetch(`${API}/shows`);
      if (!res.ok) throw new Error(`API error ${res.status}`);
      setShows(await res.json());
    } catch (e) {
      setError(e.message || 'Failed to load shows');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchShows(); }, [fetchShows]);

  const handleAddShow = async (e) => {
    e.preventDefault();
    if (!newShowInput.trim() || adding) return;
    setAdding(true);
    setAddResult(null);
    try {
      const apple_podcast_id = extractAppleId(newShowInput);
      const res = await adminFetch(`${API}/shows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ apple_podcast_id }),
      });
      const data = await res.json();
      if (!res.ok) {
        setAddResult({ success: false, message: data.detail || 'Failed to add show' });
      } else {
        setAddResult({ success: true, message: `Added "${data.title}" — queued for the next scrape` });
        setNewShowInput('');
        fetchShows();
      }
    } catch (e) {
      setAddResult({ success: false, message: e.message });
    } finally {
      setAdding(false);
    }
  };

  const handleScrapeNow = async (apple_podcast_id) => {
    setScrapingId(apple_podcast_id);
    setScrapeResults(prev => ({ ...prev, [apple_podcast_id]: null }));
    try {
      const res = await adminFetch(`${API}/shows/${apple_podcast_id}/scrape-now`, { method: 'POST' });
      const data = await res.json();
      setScrapeResults(prev => ({
        ...prev,
        [apple_podcast_id]: res.ok
          ? { success: true, message: 'Scrape started — check back in a few minutes' }
          : { success: false, message: data.detail || 'Failed to start scrape' },
      }));
    } catch (e) {
      setScrapeResults(prev => ({ ...prev, [apple_podcast_id]: { success: false, message: e.message } }));
    } finally {
      setScrapingId(null);
    }
  };

  return (
    <div className="min-h-screen bg-gray-100 font-sans">
      <header className="bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <a href="/" className="text-gray-400 hover:text-gray-600 text-sm">← Network</a>
          <a href="/admin" className="text-gray-400 hover:text-gray-600 text-sm">Suggestions</a>
          <a href="/admin/images" className="text-gray-400 hover:text-gray-600 text-sm">Images</a>
          <a href="/admin/people" className="text-gray-400 hover:text-gray-600 text-sm">People</a>
          <h1 className="text-lg font-semibold text-gray-900">Show Admin</h1>
        </div>
      </header>

      <div className="max-w-4xl mx-auto py-8 px-6">

        {/* Add a show */}
        <div className="bg-white rounded-2xl border border-gray-200 p-6 mb-6">
          <h2 className="text-base font-semibold text-gray-900 mb-3">Add a Show</h2>
          <form onSubmit={handleAddShow} className="flex gap-2">
            <input
              type="text"
              value={newShowInput}
              onChange={e => setNewShowInput(e.target.value)}
              placeholder="Apple Podcasts URL or ID"
              className="flex-1 rounded-lg border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-teal-500 focus:border-teal-500"
            />
            <button
              type="submit"
              disabled={adding}
              className="px-4 py-2 bg-teal-600 hover:bg-teal-700 disabled:opacity-50 text-white text-sm font-medium rounded-lg"
            >
              {adding ? 'Adding…' : 'Add Show'}
            </button>
          </form>
          {addResult && (
            <p className={`text-sm mt-2 ${addResult.success ? 'text-green-600' : 'text-red-500'}`}>
              {addResult.message}
            </p>
          )}
        </div>

        {/* Shows list */}
        <div className="bg-white rounded-2xl border border-gray-200 overflow-hidden">
          {loading ? (
            <div className="py-12 text-center text-gray-400 text-sm">Loading...</div>
          ) : error ? (
            <div className="py-12 flex flex-col items-center gap-3">
              <p className="text-sm font-semibold text-red-500">Couldn't load shows</p>
              <p className="text-xs text-gray-500">{error}</p>
              <button
                className="px-3 py-1.5 bg-teal-600 text-white text-sm rounded hover:bg-teal-700"
                onClick={fetchShows}
              >
                Retry
              </button>
            </div>
          ) : (
            <div className="divide-y divide-gray-100">
              {shows.map(show => {
                const badge = STATUS_BADGE[show.status] || STATUS_BADGE.pending;
                const result = scrapeResults[show.apple_podcast_id];
                return (
                  <div key={show.apple_podcast_id} className="px-6 py-4 flex items-center justify-between gap-4">
                    <div className="min-w-0">
                      <p className="font-medium text-gray-900 truncate">{show.podcast_title}</p>
                      <div className="flex items-center gap-2 mt-1 text-xs text-gray-500">
                        <span className={`px-2 py-0.5 rounded-full font-medium ${badge.bg}`}>{badge.label}</span>
                        <span>{show.episode_count} episode{show.episode_count !== 1 ? 's' : ''}</span>
                        {show.last_scraped_at && (
                          <span>· last scraped {new Date(show.last_scraped_at).toLocaleDateString()}</span>
                        )}
                      </div>
                      {show.error_message && (
                        <p className="text-xs text-red-500 mt-1 truncate">{show.error_message}</p>
                      )}
                      {result && (
                        <p className={`text-xs mt-1 ${result.success ? 'text-green-600' : 'text-red-500'}`}>
                          {result.message}
                        </p>
                      )}
                    </div>
                    <button
                      onClick={() => handleScrapeNow(show.apple_podcast_id)}
                      disabled={scrapingId === show.apple_podcast_id}
                      className="flex-shrink-0 px-3 py-1.5 bg-gray-100 hover:bg-gray-200 disabled:opacity-50 text-gray-700 text-sm font-medium rounded-lg"
                    >
                      {scrapingId === show.apple_podcast_id ? 'Starting…' : 'Scrape Now'}
                    </button>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
