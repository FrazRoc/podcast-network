import { useState, useEffect, useCallback } from 'react';

const API = 'http://localhost:8000/api/admin';

// ─── Source badge ──────────────────────────────────────────────────────────────
const SourceBadge = ({ source }) => {
  const label = source === 'parsed_title' ? 'Episode Title' : 'Description';
  const color = source === 'parsed_title'
    ? 'bg-blue-100 text-blue-700'
    : 'bg-purple-100 text-purple-700';
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${color}`}>
      {label}
    </span>
  );
};

// ─── Credit pill ───────────────────────────────────────────────────────────────
const CreditPill = ({ credit }) => {
  const isVerified = credit.data_source === 'apple_verified';
  return (
    <div className="flex items-center gap-2 py-1.5 px-3 bg-gray-50 rounded-lg text-sm">
      <span className={`w-2 h-2 rounded-full flex-shrink-0 ${credit.is_guest ? 'bg-blue-400' : 'bg-green-500'}`} />
      <span className="font-medium text-gray-800">{credit.name}</span>
      <span className="text-gray-400 text-xs">{credit.is_guest ? 'Guest' : 'Host'}</span>
      {isVerified && (
        <span className="ml-auto text-xs text-green-600 font-medium">✓ Apple</span>
      )}
    </div>
  );
};

// ─── Main component ────────────────────────────────────────────────────────────
export default function AdminSuggestions() {
  const [suggestion, setSuggestion] = useState(null);
  const [stats, setStats] = useState({});
  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState(false);
  const [lastResult, setLastResult] = useState(null);
  const [done, setDone] = useState(false);

  const fetchNext = useCallback(async () => {
    setLoading(true);
    setLastResult(null);
    try {
      const res = await fetch(`${API}/suggestions/next`);
      const data = await res.json();
      if (data.done) {
        setDone(true);
        setSuggestion(null);
      } else {
        setSuggestion(data);
        setDone(false);
      }
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchStats = useCallback(async () => {
    try {
      const res = await fetch(`${API}/suggestions/stats`);
      setStats(await res.json());
    } catch (e) {}
  }, []);

  useEffect(() => {
    fetchNext();
    fetchStats();
  }, [fetchNext, fetchStats]);

  // Keyboard shortcuts
  useEffect(() => {
    const handler = (e) => {
      if (actionLoading || !suggestion) return;
      if (e.key === 'a') handleAction('approve');
      if (e.key === 'r') handleAction('reject');
      if (e.key === 's') handleAction('skip');
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [suggestion, actionLoading]);

  const handleAction = async (action) => {
    if (!suggestion || actionLoading) return;
    setActionLoading(true);
    try {
      const res = await fetch(`${API}/suggestions/${suggestion.suggestion_id}/${action}`, {
        method: 'POST',
      });
      const result = await res.json();
      setLastResult({ action, ...result });
      await fetchStats();
      await fetchNext();
    } catch (e) {
      console.error(e);
    } finally {
      setActionLoading(false);
    }
  };

  // Highlight candidate name in description
  const highlightName = (text, name) => {
    if (!text || !name) return text;
    const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const parts = text.split(new RegExp(`(${escaped})`, 'gi'));
    return parts.map((part, i) =>
      part.toLowerCase() === name.toLowerCase()
        ? <mark key={i} className="bg-yellow-200 rounded px-0.5">{part}</mark>
        : part
    );
  };

  return (
    <div className="min-h-screen bg-gray-100 font-sans">

      {/* Header */}
      <header className="bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <a href="/" className="text-gray-400 hover:text-gray-600 text-sm">← Network</a>
          <h1 className="text-lg font-semibold text-gray-900">Suggestion Review</h1>
        </div>
        <div className="flex items-center gap-4 text-sm text-gray-500">
          {stats.pending !== undefined && (
            <>
              <span className="text-orange-600 font-medium">{stats.pending ?? 0} pending</span>
              <span>{stats.approved ?? 0} approved</span>
              <span>{stats.rejected ?? 0} rejected</span>
            </>
          )}
        </div>
      </header>

      {/* Last action result */}
      {lastResult && (
        <div className={`px-6 py-3 text-sm font-medium ${
          lastResult.action === 'approve' ? 'bg-green-50 text-green-800 border-b border-green-200' :
          lastResult.action === 'reject' ? 'bg-red-50 text-red-800 border-b border-red-200' :
          'bg-blue-50 text-blue-800 border-b border-blue-200'
        }`}>
          {lastResult.action === 'approve' && `✅ ${lastResult.message}`}
          {lastResult.action === 'reject' && `❌ Rejected "${lastResult.name}" — added to blocklist`}
          {lastResult.action === 'skip' && `⏭ Skipped — moved to back of queue`}
        </div>
      )}

      {/* Loading */}
      {loading && (
        <div className="flex items-center justify-center h-96 text-gray-400">
          Loading...
        </div>
      )}

      {/* Done */}
      {done && !loading && (
        <div className="flex flex-col items-center justify-center h-96 gap-3">
          <div className="text-4xl">🎉</div>
          <p className="text-xl font-semibold text-gray-700">Queue is empty!</p>
          <p className="text-gray-400 text-sm">Run <code className="bg-gray-100 px-1 rounded">python3 episode_name_scanner.py suggest</code> to find more.</p>
        </div>
      )}

      {/* Main layout */}
      {suggestion && !loading && (
        <div className="flex h-[calc(100vh-57px)]">

          {/* LEFT — Episode context */}
          <div className="w-1/2 border-r border-gray-200 bg-white flex flex-col">

            {/* Podcast + episode header */}
            <div className="px-6 py-5 border-b border-gray-100">
              <div className="flex items-start gap-3">
                {suggestion.podcast_cover_art && (
                  <img
                    src={suggestion.podcast_cover_art}
                    alt={suggestion.podcast_title}
                    className="w-12 h-12 rounded-lg flex-shrink-0 object-cover"
                  />
                )}
                <div className="min-w-0">
                  <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-0.5">
                    {suggestion.podcast_title}
                  </p>
                  <h2 className="text-sm font-semibold text-gray-900 leading-snug">
                    {suggestion.episode_title}
                  </h2>
                  {suggestion.published_date && (
                    <p className="text-xs text-gray-400 mt-1">
                      {new Date(suggestion.published_date).toLocaleDateString('en-US', {
                        year: 'numeric', month: 'long', day: 'numeric'
                      })}
                    </p>
                  )}
                </div>
              </div>
            </div>

            {/* Existing credits */}
            <div className="px-6 py-4 border-b border-gray-100">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
                Already Credited
              </p>
              {suggestion.existing_credits?.length > 0 ? (
                <div className="space-y-1.5">
                  {suggestion.existing_credits.map((c, i) => (
                    <CreditPill key={i} credit={c} />
                  ))}
                </div>
              ) : (
                <p className="text-sm text-gray-400 italic">No credits yet on this episode</p>
              )}
            </div>

            {/* Full episode description */}
            <div className="px-6 py-4 flex-1 overflow-y-auto">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
                Episode Description
              </p>
              <div className="text-sm text-gray-700 leading-relaxed whitespace-pre-wrap">
                {highlightName(suggestion.episode_description, suggestion.candidate_name)}
              </div>
            </div>
          </div>

          {/* RIGHT — Suggestion + actions */}
          <div className="w-1/2 flex flex-col">

            {/* Candidate */}
            <div className="px-8 py-8 border-b border-gray-200 bg-white">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-3">
                Suggested Person
              </p>
              <div className="flex items-baseline gap-3 mb-3">
                <h2 className="text-3xl font-bold text-gray-900">
                  {suggestion.candidate_name}
                </h2>
                <SourceBadge source={suggestion.source} />
              </div>

              {/* Matched context */}
              <div className="bg-yellow-50 border border-yellow-200 rounded-lg px-4 py-3 mb-4">
                <p className="text-xs font-semibold text-yellow-700 mb-1">Matched text</p>
                <p className="text-sm text-yellow-900 italic">
                  "…{suggestion.matched_text}…"
                </p>
              </div>

              {/* Other pending suggestions for this name */}
              {suggestion.other_pending_suggestions > 0 && (
                <p className="text-sm text-blue-600">
                  + {suggestion.other_pending_suggestions} other episode{suggestion.other_pending_suggestions !== 1 ? 's' : ''} also mention this name — approving will link all of them.
                </p>
              )}
            </div>

            {/* Action buttons */}
            <div className="px-8 py-8 bg-gray-50 flex-1">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-6">
                Your Decision
              </p>

              <div className="space-y-3">
                <button
                  onClick={() => handleAction('approve')}
                  disabled={actionLoading}
                  className="w-full py-4 px-6 bg-green-600 hover:bg-green-700 disabled:opacity-50 text-white font-semibold rounded-xl text-lg transition-colors flex items-center justify-between"
                >
                  <span>✅ Approve</span>
                  <span className="text-green-300 text-sm font-normal">press A</span>
                </button>

                <button
                  onClick={() => handleAction('reject')}
                  disabled={actionLoading}
                  className="w-full py-4 px-6 bg-red-600 hover:bg-red-700 disabled:opacity-50 text-white font-semibold rounded-xl text-lg transition-colors flex items-center justify-between"
                >
                  <span>❌ Reject + Blocklist</span>
                  <span className="text-red-300 text-sm font-normal">press R</span>
                </button>

                <button
                  onClick={() => handleAction('skip')}
                  disabled={actionLoading}
                  className="w-full py-4 px-6 bg-gray-200 hover:bg-gray-300 disabled:opacity-50 text-gray-700 font-semibold rounded-xl text-lg transition-colors flex items-center justify-between"
                >
                  <span>⏭ Skip for now</span>
                  <span className="text-gray-400 text-sm font-normal">press S</span>
                </button>
              </div>

              {/* Progress */}
              <div className="mt-8 pt-6 border-t border-gray-200">
                <div className="flex justify-between text-sm text-gray-500 mb-2">
                  <span>Queue progress</span>
                  <span className="font-medium">{suggestion.total_pending} remaining</span>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-2">
                  <div
                    className="bg-green-500 h-2 rounded-full transition-all"
                    style={{
                      width: `${Math.min(100, ((stats.approved ?? 0) /
                        Math.max(1, (stats.approved ?? 0) + (stats.rejected ?? 0) + suggestion.total_pending)) * 100)}%`
                    }}
                  />
                </div>
                <div className="flex justify-between text-xs text-gray-400 mt-1">
                  <span>{(stats.approved ?? 0) + (stats.rejected ?? 0)} reviewed</span>
                  <span>{stats.approved ?? 0} approved · {stats.rejected ?? 0} rejected</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
