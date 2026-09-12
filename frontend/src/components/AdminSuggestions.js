import { useState, useEffect, useCallback } from 'react';
import { API_BASE_URL } from '../config';
import { adminFetch } from '../adminAuth';

const API = `${API_BASE_URL}/api/admin`;

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
  const color = credit.is_guest ? 'bg-blue-400' : 'bg-green-500';
  return (
    <div className="flex items-center gap-2 py-1.5 px-3 bg-gray-50 rounded-lg text-sm">
      {credit.profile_image_url ? (
        <img
          src={credit.profile_image_url}
          alt={credit.name}
          className="w-6 h-6 rounded-full object-cover flex-shrink-0"
          onError={e => { e.target.style.display='none'; }}
        />
      ) : (
        <span className={`w-2 h-2 rounded-full flex-shrink-0 ${color}`} />
      )}
      <span className={`font-medium ${credit.is_guest ? 'text-blue-700' : 'text-green-700'}`}>{credit.name}</span>
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
  const [editedName, setEditedName] = useState('');
  const [done, setDone] = useState(false);

  const fetchNext = useCallback(async (clearResult = true) => {
    setLoading(true);
    if (clearResult) setLastResult(null);
    try {
      const res = await adminFetch(`${API}/suggestions/next`);
      const data = await res.json();
      if (data.done) {
        setDone(true);
        setSuggestion(null);
      } else {
        setSuggestion(data);
        setEditedName('');
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
      const res = await adminFetch(`${API}/suggestions/stats`);
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
      if (e.key === 'p') handleAction('approve_only');
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
      const body = editedName.trim() && editedName.trim() !== suggestion.candidate_name
        ? JSON.stringify({ name: editedName.trim() })
        : undefined;
      const res = await adminFetch(`${API}/suggestions/${suggestion.suggestion_id}/${action}`, {
        method: 'POST',
        headers: body ? { 'Content-Type': 'application/json' } : {},
        body,
      });
      const result = await res.json();
      setLastResult({ action, ...result });
      await fetchStats();
      await fetchNext(false); // don't clear result when loading next
    } catch (e) {
      console.error(e);
    } finally {
      setActionLoading(false);
    }
  };

  // Highlight names in text with configurable style
  const highlightNames = (text, highlights) => {
    // highlights: [{name, className}]
    if (!text || !highlights?.length) return text;
    
    // Build a combined regex for all names
    const patterns = highlights.map(h => ({
      re: new RegExp(h.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi'),
      className: h.className,
    }));

    // Find all matches with positions
    const matches = [];
    patterns.forEach(({ re, className }) => {
      let m;
      while ((m = re.exec(text)) !== null) {
        matches.push({ start: m.index, end: m.index + m[0].length, text: m[0], className });
      }
    });

    if (!matches.length) return text;

    matches.sort((a, b) => a.start - b.start);

    const parts = [];
    let pos = 0;
    matches.forEach((m, i) => {
      if (m.start > pos) parts.push(text.slice(pos, m.start));
      parts.push(<mark key={i} className={m.className}>{m.text}</mark>);
      pos = m.end;
    });
    if (pos < text.length) parts.push(text.slice(pos));
    return parts;
  };

  return (
    <div className="min-h-screen bg-gray-100 font-sans">

      {/* Header */}
      <header className="bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <a href="/" className="text-gray-400 hover:text-gray-600 text-sm">← Network</a>
          <a href="/admin/images" className="text-gray-400 hover:text-gray-600 text-sm">Images</a>
          <a href="/admin/people" className="text-gray-400 hover:text-gray-600 text-sm">People</a>
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
              <div className="flex items-center justify-between mb-2">
                <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide">
                  Episode Description
                </p>
                {suggestion.other_pending_names?.length > 0 && (
                  <p className="text-xs text-purple-600">
                    <span className="underline decoration-dotted decoration-purple-500 decoration-2">dotted</span> = also pending review
                  </p>
                )}
              </div>
              <div className="text-sm text-gray-700 leading-relaxed whitespace-pre-wrap text-left">
                {highlightNames(suggestion.episode_description, [
                  { name: suggestion.candidate_name, className: 'bg-yellow-200 rounded px-0.5 not-italic' },
                  ...(suggestion.existing_credits || []).map(c => ({
                    name: c.name,
                    className: c.is_guest
                      ? 'underline decoration-blue-400 decoration-2 bg-transparent not-italic'
                      : 'underline decoration-green-500 decoration-2 bg-transparent not-italic',
                  })),
                  ...(suggestion.other_pending_names || []).map(name => ({
                    name,
                    className: 'underline decoration-dotted decoration-purple-500 decoration-2 bg-transparent not-italic',
                  })),
                ])}
              </div>
            </div>
          </div>

          {/* RIGHT — Suggestion + actions */}
          <div className="w-1/2 flex flex-col">

            {/* Candidate */}
            <div className="px-6 py-5 border-b border-gray-200 bg-white">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
                Suggested Person
              </p>
              <div className="flex items-baseline gap-3 mb-2">
                <SourceBadge source={suggestion.source} />
              </div>
              <div className="flex items-center gap-2 mb-2">
                <input
                  type="text"
                  value={editedName || suggestion.candidate_name}
                  onChange={e => setEditedName(e.target.value)}
                  className="text-2xl font-bold text-gray-900 bg-transparent border-b-2 border-transparent hover:border-gray-300 focus:border-blue-500 focus:outline-none w-full"
                />
              </div>
              {editedName && editedName !== suggestion.candidate_name && (
                <p className="text-xs text-blue-600 mb-2">✏️ Name edited — will approve as "{editedName}"</p>
              )}

              {/* Matched context */}
              <div className="bg-yellow-50 border border-yellow-200 rounded-lg px-4 py-2.5 mb-3">
                <p className="text-xs font-semibold text-yellow-700 mb-1">Matched text</p>
                <p className="text-sm text-yellow-900 italic">
                  "…{highlightNames(suggestion.matched_text, [
                    { name: suggestion.candidate_name, className: 'bg-yellow-300 rounded px-0.5 not-italic font-semibold' }
                  ])}…"
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
            <div className="px-6 py-5 bg-gray-50 flex-1">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-3">
                Your Decision
              </p>

              <div className="space-y-2">
                <button
                  onClick={() => handleAction('approve')}
                  disabled={actionLoading}
                  className="w-full py-2.5 px-5 bg-green-600 hover:bg-green-700 disabled:opacity-50 text-white font-semibold rounded-lg text-base transition-colors flex items-center justify-between"
                >
                  <span>✅ Approve</span>
                  <span className="text-green-300 text-sm font-normal">press A</span>
                </button>

                <button
                  onClick={() => handleAction('approve_only')}
                  disabled={actionLoading}
                  className="w-full py-2.5 px-5 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white font-semibold rounded-lg text-base transition-colors flex items-center justify-between"
                >
                  <span>👤 Approve Person Only</span>
                  <span className="text-blue-300 text-sm font-normal">press P</span>
                </button>

                <button
                  onClick={() => handleAction('reject')}
                  disabled={actionLoading}
                  className="w-full py-2.5 px-5 bg-red-600 hover:bg-red-700 disabled:opacity-50 text-white font-semibold rounded-lg text-base transition-colors flex items-center justify-between"
                >
                  <span>❌ Reject + Blocklist</span>
                  <span className="text-red-300 text-sm font-normal">press R</span>
                </button>

                <button
                  onClick={() => handleAction('skip')}
                  disabled={actionLoading}
                  className="w-full py-2.5 px-5 bg-gray-200 hover:bg-gray-300 disabled:opacity-50 text-gray-700 font-semibold rounded-lg text-base transition-colors flex items-center justify-between"
                >
                  <span>⏭ Skip for now</span>
                  <span className="text-gray-400 text-sm font-normal">press S</span>
                </button>
              </div>

              {/* Last action result — persists until next action */}
              {lastResult && (
                <div className={`mt-4 p-3 rounded-lg text-sm ${
                  lastResult.action === 'approve' || lastResult.action === 'approve_only'
                    ? 'bg-green-50 border border-green-200'
                    : lastResult.action === 'reject'
                    ? 'bg-red-50 border border-red-200'
                    : 'bg-gray-100 border border-gray-200'
                }`}>
                  {(lastResult.action === 'approve' || lastResult.action === 'approve_only') && (
                    <>
                      <p className="font-semibold text-green-800 mb-1">
                        {lastResult.action === 'approve_only' ? '👤 Person created (not linked to this episode):' : '✅ Approved:'} {lastResult.name}
                      </p>
                      <p className="text-green-700">
                        {lastResult.additional_episodes_linked > 0
                          ? `Found ${lastResult.additional_episodes_linked} additional episode appearance(s):`
                          : 'No additional appearances found in other episodes.'}
                      </p>
                      {lastResult.by_podcast && Object.keys(lastResult.by_podcast).length > 0 && (
                        <ul className="mt-2 space-y-0.5">
                          {Object.entries(lastResult.by_podcast).map(([show, count]) => (
                            <li key={show} className="text-green-700">
                              • {show} <span className="font-medium">({count} episode{count !== 1 ? 's' : ''})</span>
                            </li>
                          ))}
                        </ul>
                      )}
                    </>
                  )}
                  {lastResult.action === 'reject' && (
                    <p className="font-semibold text-red-800">
                      ❌ Rejected "{lastResult.name}" — added to permanent blocklist
                    </p>
                  )}
                  {lastResult.action === 'skip' && (
                    <p className="font-semibold text-gray-600">
                      ⏭ Skipped — moved to back of queue
                    </p>
                  )}
                </div>
              )}

              {/* Progress */}
              <div className="mt-5 pt-4 border-t border-gray-200">
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
