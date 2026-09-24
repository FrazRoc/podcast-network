import { useState, useEffect, useCallback } from 'react';
import { API_BASE_URL } from '../config';
import { adminFetch } from '../adminAuth';
import AdminHeader from './AdminHeader';

const API = `${API_BASE_URL}/api/admin`;

const CONFIDENCE = {
  strong: { label: 'Strong', cls: 'bg-teal-50 text-teal-700 border-teal-200' },
  likely: { label: 'Likely', cls: 'bg-amber-50 text-amber-700 border-amber-200' },
  review: { label: 'Needs review', cls: 'bg-gray-100 text-gray-600 border-gray-200' },
};

const KIND_LABEL = {
  shortened: 'shortened first name',
  nickname: 'nickname',
  middle_name: 'middle name',
  initial: 'initial',
  compound_surname: 'missing surname word',
};

function Evidence({ pair }) {
  const bits = [];
  if (pair.same_social) bits.push('same social handle');
  if (pair.shared_episodes > 0)
    bits.push(`credited together on ${pair.shared_episodes} episode${pair.shared_episodes === 1 ? '' : 's'}`);
  if (pair.shared_shows > 0)
    bits.push(`${pair.shared_shows} show${pair.shared_shows === 1 ? '' : 's'} in common`);
  if (!bits.length) return <span className="text-gray-400">no supporting signal — judge by name</span>;
  return <span>{bits.join(' · ')}</span>;
}

export default function AdminDuplicates() {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [busyKey, setBusyKey] = useState(null);
  const [results, setResults] = useState([]);
  // Which record survives, when the user overrides the suggested default.
  const [flipped, setFlipped] = useState({});

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await adminFetch(`${API}/people/duplicates`);
      if (!res.ok) throw new Error(`API error ${res.status}`);
      const data = await res.json();
      setItems(data.items || []);
    } catch (e) {
      setError(e.message || 'Failed to load');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const keyOf = (p) => `${p.suggested_keep_id}-${p.suggested_drop_id}`;

  const resolve = (pair) => {
    const flip = flipped[keyOf(pair)];
    return flip
      ? { keepId: pair.suggested_drop_id,  keepName: pair.suggested_drop_name,
          keepCredits: pair.drop_credits,  keepShows: pair.drop_shows,
          dropId: pair.suggested_keep_id,  dropName: pair.suggested_keep_name,
          dropCredits: pair.keep_credits,  dropShows: pair.keep_shows }
      : { keepId: pair.suggested_keep_id,  keepName: pair.suggested_keep_name,
          keepCredits: pair.keep_credits,  keepShows: pair.keep_shows,
          dropId: pair.suggested_drop_id,  dropName: pair.suggested_drop_name,
          dropCredits: pair.drop_credits,  dropShows: pair.drop_shows };
  };

  const handleMerge = async (pair) => {
    const { keepId, keepName, dropId, dropName } = resolve(pair);
    if (!window.confirm(
      `Merge "${dropName}" into "${keepName}"?\n\n` +
      `${dropName} will be deleted and kept as an alias, so future episodes ` +
      `using that spelling still credit ${keepName}.`
    )) return;

    setBusyKey(keyOf(pair));
    try {
      const res = await adminFetch(`${API}/people/${keepId}/merge/${dropId}`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `API error ${res.status}`);
      setResults(r => [{ ...data, key: keyOf(pair) }, ...r]);
      setItems(list => list.filter(p => keyOf(p) !== keyOf(pair)));
    } catch (e) {
      alert(e.message || 'Merge failed');
    } finally {
      setBusyKey(null);
    }
  };

  const handleDismiss = async (pair) => {
    setBusyKey(keyOf(pair));
    try {
      const res = await adminFetch(`${API}/people/duplicates/dismiss`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ host_id_a: pair.suggested_keep_id, host_id_b: pair.suggested_drop_id }),
      });
      if (!res.ok) throw new Error(`API error ${res.status}`);
      setItems(list => list.filter(p => keyOf(p) !== keyOf(pair)));
    } catch (e) {
      alert(e.message || 'Could not dismiss');
    } finally {
      setBusyKey(null);
    }
  };

  const grouped = ['strong', 'likely', 'review']
    .map(c => [c, items.filter(i => i.confidence === c)])
    .filter(([, list]) => list.length > 0);

  return (
    <div className="min-h-screen bg-gray-100 font-sans">
      <AdminHeader
        active="Duplicates"
        right={<span className="text-sm text-gray-500">{items.length} possible duplicate{items.length === 1 ? '' : 's'}</span>}
      />

      <div className="max-w-4xl mx-auto p-4 sm:p-6 space-y-6">
        <p className="text-sm text-gray-500">
          People who may be the same person written two ways. Nothing is merged automatically —
          a shortened first name isn't proof, so each pair needs your call. Merging keeps the
          discarded spelling as an alias, so future scans still recognise it.
        </p>

        {results.map(r => (
          <div key={r.key} className="bg-teal-50 border border-teal-200 rounded-xl p-3 text-sm text-teal-900">
            Merged <strong>{r.merged}</strong> into <strong>{r.kept}</strong>
            {r.alias_added && <> · kept "{r.merged}" as an alias</>}
            {r.credits_moved > 0 && <> · {r.credits_moved} credit{r.credits_moved === 1 ? '' : 's'} moved</>}
            {r.duplicate_credits_removed > 0 && <> · {r.duplicate_credits_removed} duplicate removed</>}
            {r.labels_corrected > 0 && <> · {r.labels_corrected} label{r.labels_corrected === 1 ? '' : 's'} corrected from Apple</>}
            {r.roles_reconciled > 0 && <> · {r.roles_reconciled} role{r.roles_reconciled === 1 ? '' : 's'} reconciled</>}
          </div>
        ))}

        {loading && <p className="text-sm text-gray-400">Loading…</p>}
        {error && <p className="text-sm text-red-500">Couldn't load: {error}</p>}
        {!loading && !error && items.length === 0 && (
          <p className="text-sm text-gray-500">No possible duplicates outstanding.</p>
        )}

        {grouped.map(([confidence, list]) => (
          <div key={confidence}>
            <h2 className="text-sm font-semibold text-gray-700 mb-2">
              {CONFIDENCE[confidence].label}
              <span className="ml-2 font-normal text-gray-400">{list.length}</span>
            </h2>
            <div className="space-y-3">
              {list.map(pair => {
                const k = keyOf(pair);
                const r = resolve(pair);
                const busy = busyKey === k;
                const flip = !!flipped[k];
                return (
                  <div key={k} className="bg-white rounded-2xl border border-gray-200 p-4">
                    <div className="flex flex-wrap items-center gap-2 mb-3">
                      <span className={`text-xs px-2 py-0.5 rounded-full border ${CONFIDENCE[pair.confidence].cls}`}>
                        {CONFIDENCE[pair.confidence].label}
                      </span>
                      <span className="text-xs text-gray-400">{KIND_LABEL[pair.kind] || pair.kind}</span>
                    </div>

                    {/* Both names link out (new tab) so you can compare the two
                        records before choosing which one survives. */}
                    <div className="flex flex-col sm:flex-row sm:items-baseline gap-1 sm:gap-3 mb-2">
                      <span className="flex items-baseline gap-2">
                        <a href={`/admin/people?host_id=${r.keepId}`}
                           target="_blank" rel="noopener noreferrer"
                           className="text-base font-semibold text-gray-900 hover:text-teal-700 underline decoration-gray-300 underline-offset-2">
                          {r.keepName}
                        </a>
                        <span className="text-xs text-gray-400">
                          keeps · {r.keepCredits} ep, {r.keepShows} shows
                        </span>
                      </span>
                      <span className="text-gray-300 hidden sm:inline">←</span>
                      <span className="flex items-baseline gap-2">
                        <a href={`/admin/people?host_id=${r.dropId}`}
                           target="_blank" rel="noopener noreferrer"
                           className="text-base text-gray-500 line-through hover:text-red-600 underline decoration-gray-300 underline-offset-2">
                          {r.dropName}
                        </a>
                        <span className="text-xs text-gray-400">
                          merged · {r.dropCredits} ep, {r.dropShows} shows
                        </span>
                      </span>
                    </div>

                    <p className="text-xs text-gray-500 mb-1">
                      <Evidence pair={pair} />
                    </p>
                    <p className="text-xs text-gray-400 mb-3">
                      {flip
                        ? 'Keeping the other record (your choice).'
                        : <>Suggested keep: {pair.keep_reason}.</>}
                    </p>

                    <div className="flex flex-wrap gap-2">
                      <button
                        onClick={() => handleMerge(pair)}
                        disabled={busy}
                        className="px-3 py-1.5 text-sm rounded-lg bg-teal-600 text-white hover:bg-teal-700 disabled:opacity-50"
                      >
                        {busy ? 'Merging…' : 'Merge'}
                      </button>
                      <button
                        onClick={() => setFlipped(f => ({ ...f, [k]: !f[k] }))}
                        disabled={busy}
                        className="px-3 py-1.5 text-sm rounded-lg border border-gray-300 text-gray-600 hover:bg-gray-50 disabled:opacity-50"
                      >
                        Keep the other one
                      </button>
                      <button
                        onClick={() => handleDismiss(pair)}
                        disabled={busy}
                        className="px-3 py-1.5 text-sm rounded-lg border border-gray-300 text-gray-600 hover:bg-gray-50 disabled:opacity-50"
                      >
                        Not a duplicate
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
