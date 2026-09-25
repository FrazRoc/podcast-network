import React, { useState, useEffect, useCallback, useRef } from 'react';
import { API_BASE_URL } from '../config';
import { adminFetch } from '../adminAuth';
import AdminHeader from './AdminHeader';
import AdminSubTabs from './AdminSubTabs';
import AdminListCount from './AdminListCount';
import OrgLogo from './OrgLogo';

const API = `${API_BASE_URL}/api/admin`;
const PAGE_SIZE = 100;

// Duplicates is its own page (AdminDuplicates) under the People section.
export const PEOPLE_TABS = [
  { id: 'people',     label: 'People',     href: '/admin/people' },
  { id: 'duplicates', label: 'Duplicates', href: '/admin/duplicates' },
];

const PROXY = (url) =>
  url && !url.includes('mzstatic.com') && !url.includes('cdn.bsky.app')
    ? `${API_BASE_URL}/api/proxy/image?url=${encodeURIComponent(url)}`
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
  // 5+ guest credits piled onto 3 or fewer shows — not proof of anything on
  // its own, but the shape a real over-crediting incident keeps taking
  // (mentioned in a footer/citation repeatedly, or a misfiled host) more
  // often than a genuine wide-ranging recurring guest does. A worklist to
  // review, not a verdict.
  { id: 'concentrated', label: 'Concentrated (5+ on ≤3 shows)' },
  // On the current role shown on their profile (pinned, else derived).
  { id: 'role_title_company', label: 'Has role & company' },
  { id: 'role_title_only',    label: 'Has role only' },
  { id: 'role_company_only',  label: 'Has company only' },
  { id: 'role_none',          label: 'No role or company' },
];

const SORTS = [
  { id: 'appearances_desc', label: 'Most episodes' },
  { id: 'appearances_asc',  label: 'Fewest episodes' },
  { id: 'shows_desc',       label: 'Most shows' },
  { id: 'name_asc',         label: 'Name A–Z' },
  { id: 'name_desc',        label: 'Name Z–A' },
  { id: 'newest',           label: 'Newest first' },
  { id: 'company_asc',      label: 'Company A–Z' },
];

const emptyForm = { first_name: '', last_name: '', twitter_url: '', bluesky_url: '', linkedin_url: '' };

// ─── Right panel: Add or Edit ──────────────────────────────────────────────────
// Other spellings of this person's name. Scans match these as well as the
// canonical name, so an episode saying "Nat Bullard" credits Nathaniel Bullard
// instead of creating a second record.
function AliasEditor({ hostId }) {
  const [items, setItems] = useState([]);
  const [value, setValue] = useState('');
  const [busy, setBusy]   = useState(false);
  const [error, setError] = useState('');

  const latest = useRef(0);  // newest request wins; see PersonPanel
  const load = useCallback(() => {
    const token = ++latest.current;
    adminFetch(`${API}/people/${hostId}/aliases`)
      .then(r => r.json())
      .then(d => { if (token === latest.current) setItems(d.items || []); })
      .catch(console.error);
  }, [hostId]);

  useEffect(() => { setItems([]); }, [hostId]);
  useEffect(() => { load(); }, [load]);

  const add = async () => {
    const name = value.trim();
    if (!name || busy) return;
    setBusy(true); setError('');
    try {
      const res = await adminFetch(`${API}/people/${hostId}/aliases`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ alias_name: name }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `API error ${res.status}`);
      setValue('');
      load();
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy(false);
    }
  };

  const remove = async (aliasId) => {
    setBusy(true);
    try {
      await adminFetch(`${API}/people/aliases/${aliasId}`, { method: 'DELETE' });
      load();
    } catch (e) { console.error(e); }
    finally { setBusy(false); }
  };

  return (
    <div className="mb-4">
      <label className="block text-xs font-medium text-gray-500 mb-1">
        Also known as
      </label>
      <p className="text-xs text-gray-400 mb-2">
        Other spellings found in episode text. Scans match these too.
      </p>
      {items.length > 0 && (
        <div className="flex flex-wrap gap-1.5 mb-2">
          {items.map(a => (
            <span key={a.alias_id}
              className="inline-flex items-center gap-1 text-xs bg-gray-100 text-gray-700 rounded-full pl-2.5 pr-1 py-1">
              {a.alias_name}
              <button onClick={() => remove(a.alias_id)} disabled={busy}
                className="text-gray-400 hover:text-red-600 px-1" title="Remove">×</button>
            </span>
          ))}
        </div>
      )}
      <div className="flex gap-1.5">
        <input type="text" value={value}
          onChange={e => setValue(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter') { e.preventDefault(); add(); } }}
          placeholder="e.g. Nat Bullard"
          className="flex-1 rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
        <button onClick={add} disabled={busy || !value.trim()}
          className="px-3 py-2 text-sm rounded-lg border border-gray-300 text-gray-600 hover:bg-gray-50 disabled:opacity-50">
          Add
        </button>
      </div>
      {error && <p className="text-xs text-red-500 mt-1">{error}</p>}
    </div>
  );
}

// Job title and organisation. "Current role" is what the public profile shows:
// derived from the newest appearance (backend/role_selection.py) unless pinned
// here. History is every role read from episode text, newest first — raw, as
// each show worded it.
const formatRole = (r) => [r?.title, r?.company].filter(Boolean).join(' @ ');
// A history row keeps the show's own wording; `company` is the organisation's
// name (blank when it is not an organisation).
const formatHistoryRole = (r) => formatRole({ ...r, company: r.company && (r.company_as_written || r.company) });

function RoleEditor({ hostId }) {
  const [data, setData]       = useState(null);
  const [editing, setEditing] = useState(false);
  const [form, setForm]       = useState({ title: '', company: '' });
  const [busy, setBusy]       = useState(false);
  const [error, setError]     = useState('');
  const [showAll, setShowAll] = useState(false);

  const latest = useRef(0);  // newest request wins; see PersonPanel
  const load = useCallback(() => {
    const token = ++latest.current;
    adminFetch(`${API}/people/${hostId}/roles`)
      .then(r => r.json())
      .then(d => { if (token === latest.current) setData(d); })
      .catch(console.error);
  }, [hostId]);

  useEffect(() => { setData(null); }, [hostId]);
  useEffect(() => { load(); setEditing(false); setShowAll(false); setError(''); }, [load]);

  const startEdit = () => {
    const c = data?.current;
    setForm({ title: c?.title || '', company: c?.company || '' });
    setEditing(true);
    setError('');
  };

  const save = async () => {
    setBusy(true); setError('');
    try {
      const res = await adminFetch(`${API}/people/${hostId}/role-pin`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(form),
      });
      const d = await res.json();
      if (!res.ok) throw new Error(d.detail || `API error ${res.status}`);
      setEditing(false);
      load();
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy(false);
    }
  };

  const clearPin = async () => {
    setBusy(true); setError('');
    try {
      await adminFetch(`${API}/people/${hostId}/role-pin`, { method: 'DELETE' });
      load();
    } catch (e) { setError(e.message); }
    finally { setBusy(false); }
  };

  if (!data) return <p className="text-xs text-gray-400 mb-4">Loading role…</p>;
  const { current, derived, history = [] } = data;
  const pinned = current?.source === 'pinned';
  const shown = showAll ? history : history.slice(0, 5);

  return (
    <div className="mb-4">
      <label className="block text-xs font-medium text-gray-500 mb-1">Current role</label>
      {!editing ? (
        <div className="flex items-start justify-between gap-2 bg-gray-50 rounded-lg px-3 py-2 mb-2">
          <div className="text-sm">
            {current ? (
              <p className="text-gray-900">{formatRole(current)}</p>
            ) : (
              <p className="text-gray-400">None on record</p>
            )}
            <p className="text-xs text-gray-400 mt-0.5">
              {pinned
                ? <>Pinned by hand{derived ? <> · rule would show “{formatRole(derived)}”</> : null}</>
                : current
                  ? <>From {current.podcast_title}{current.published_date ? `, ${current.published_date}` : ''}</>
                  : 'Nothing extracted from episode text yet'}
            </p>
          </div>
          <div className="flex gap-2 flex-shrink-0 text-xs">
            <button onClick={startEdit} className="text-blue-600 hover:underline">
              {pinned ? 'Edit pin' : 'Pin'}
            </button>
            {pinned && (
              <button onClick={clearPin} disabled={busy} className="text-gray-500 hover:text-red-600">
                Unpin
              </button>
            )}
          </div>
        </div>
      ) : (
        <div className="bg-gray-50 rounded-lg p-3 mb-2 space-y-2">
          <input type="text" value={form.title} placeholder="Title (e.g. CEO)"
            onChange={e => setForm(f => ({ ...f, title: e.target.value }))}
            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
          <input type="text" value={form.company} placeholder="Company"
            onChange={e => setForm(f => ({ ...f, company: e.target.value }))}
            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
          <div className="flex gap-2">
            <button onClick={save} disabled={busy || (!form.title.trim() && !form.company.trim())}
              className="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">
              Save pin
            </button>
            <button onClick={() => setEditing(false)}
              className="px-3 py-1.5 text-sm rounded-lg border border-gray-300 text-gray-600 hover:bg-gray-50">
              Cancel
            </button>
          </div>
        </div>
      )}
      {error && <p className="text-xs text-red-500 mb-1">{error}</p>}

      {history.length > 0 && (
        <div className="mt-2">
          <p className="text-xs font-medium text-gray-500 mb-1">Roles from episodes ({history.length})</p>
          <ul className="space-y-1">
            {shown.map(r => (
              <li key={r.affiliation_id} className="text-xs text-gray-600">
                <span className={r.is_former ? 'text-gray-400' : 'text-gray-800'}>{formatHistoryRole(r)}</span>
                {r.is_former && <span className="ml-1 text-gray-400">(former)</span>}
                {r.title_kind === 'description' && <span className="ml-1 text-gray-400">(description)</span>}
                {r.from_other_episode && (
                  <span className="ml-1 text-amber-600" title="Text refers to another episode or a rerun">other episode</span>
                )}
                {r.appears_on_episode === false && (
                  <span className="ml-1 text-amber-600" title="Only talked about, not appearing">mentioned only</span>
                )}
                <span className="block text-gray-400">
                  {r.podcast_title}{r.published_date ? ` · ${r.published_date}` : ''}
                </span>
              </li>
            ))}
          </ul>
          {history.length > 5 && (
            <button onClick={() => setShowAll(v => !v)} className="text-xs text-blue-600 hover:underline mt-1">
              {showAll ? 'Show fewer' : `Show all ${history.length}`}
            </button>
          )}
        </div>
      )}
    </div>
  );
}

function PersonPanel({ selected, onSaved, onCancel }) {
  const [form, setForm]         = useState(emptyForm);
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult]     = useState(null);
  const [error, setError]       = useState('');
  const [existingId, setExistingId] = useState(null);
  const [episodes, setEpisodes] = useState([]);
  const [expandedShows, setExpandedShows] = useState({});
  const [creditActionId, setCreditActionId] = useState(null); // episode_id currently being acted on
  const isEdit = !!selected;

  // Only the newest request may fill the panel, so a slow response for a
  // row clicked earlier can't overwrite the one clicked since.
  const latest = useRef(0);
  const loadEpisodes = useCallback(() => {
    const token = ++latest.current;
    if (!selected) { setEpisodes([]); return; }
    adminFetch(`${API}/people/${selected.host_id}/episodes`)
      .then(r => r.json())
      .then(e => { if (token === latest.current) setEpisodes(e); })
      .catch(console.error);
  }, [selected]);

  useEffect(() => {
    // A different person: show Loading at once rather than the previous one's.
    setEpisodes(null);
    loadEpisodes();
  // Keyed on the id rather than the object, which is a new reference each render.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected?.host_id]);

  // Same two endpoints the Episodes admin page's credit management already
  // uses — a credit removed this way is permanently suppressed against
  // re-insertion (see remove_episode_credit's docstring), and a reclassify
  // is just the add-credit upsert with is_guest flipped.
  const handleRemoveCredit = async (episodeId) => {
    setCreditActionId(episodeId);
    try {
      const res = await adminFetch(`${API}/episodes/${episodeId}/credits/${selected.host_id}`, { method: 'DELETE' });
      if (!res.ok) throw new Error((await res.json()).detail || 'Failed to remove credit');
      loadEpisodes();
    } catch (e) {
      setError(e.message);
    } finally {
      setCreditActionId(null);
    }
  };

  const handleReclassifyToHost = async (episodeId) => {
    setCreditActionId(episodeId);
    try {
      const res = await adminFetch(`${API}/episodes/${episodeId}/credits`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ host_id: selected.host_id, is_guest: false }),
      });
      if (!res.ok) throw new Error((await res.json()).detail || 'Failed to reclassify');
      loadEpisodes();
    } catch (e) {
      setError(e.message);
    } finally {
      setCreditActionId(null);
    }
  };

  const toggleShow = (show) =>
    setExpandedShows(prev => ({ ...prev, [show]: !prev[show] }));

  // Track previous host_id to only reset result when switching to a DIFFERENT person
  const prevHostId = React.useRef(null);
  useEffect(() => {
    const switchedPerson = selected?.host_id !== prevHostId.current;
    prevHostId.current = selected?.host_id || null;

    if (selected) {
      setForm({
        first_name:   selected.first_name  || '',
        last_name:    selected.last_name   || '',
        twitter_url:  selected.twitter_handle ? `https://x.com/${selected.twitter_handle}` : '',
        bluesky_url:  selected.bluesky_handle  ? `https://bsky.app/profile/${selected.bluesky_handle}` : '',
        linkedin_url: selected.linkedin_url || '',
      });
      setError('');
      setExistingId(null);
      if (switchedPerson) setResult(null);  // only clear result when switching people
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
        res  = await adminFetch(`${API}/people/${selected.host_id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(form),
        });
      } else {
        res = await adminFetch(`${API}/people`, {
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
        // Pass back updated person data so parent can refresh selected
        onSaved?.({
          host_id: data.host_id || selected?.host_id,
          profile_image_url: data.image_url || selected?.profile_image_url,
          first_name: form.first_name.trim(),
          last_name: form.last_name.trim(),
          full_name: `${form.first_name.trim()} ${form.last_name.trim()}`,
          twitter_handle: form.twitter_url ? form.twitter_url.split('/').pop() : selected?.twitter_handle,
          bluesky_handle: form.bluesky_url ? form.bluesky_url.split('/').pop() : selected?.bluesky_handle,
          linkedin_url: form.linkedin_url || selected?.linkedin_url,
        });
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
      const res  = await adminFetch(`${API}/people/${hostId}/scan`, { method: 'POST' });
      const data = await res.json();
      setResult(data);
      setExistingId(null);
      onSaved?.({ host_id: data.host_id, profile_image_url: data.image_url });
    } catch (e) {
      setError(e.message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="bg-white rounded-2xl border border-gray-200 p-6 overflow-y-auto md:sticky md:top-6 md:max-h-[calc(100vh-80px)]">
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

      {isEdit && <RoleEditor hostId={selected.host_id} />}

      {isEdit && <AliasEditor hostId={selected.host_id} />}

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
          <div className="flex gap-1.5">
            <input type="text" value={form.twitter_url}
              onChange={e => setForm(f => ({ ...f, twitter_url: e.target.value }))}
              placeholder="https://x.com/handle"
              className="flex-1 rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
            <a
              href={`https://x.com/search?q=${encodeURIComponent((form.first_name + ' ' + form.last_name).trim())}&src=typed_query&f=user`}
              target="_blank" rel="noopener noreferrer"
              className="px-2.5 py-2 bg-gray-50 hover:bg-gray-100 border border-gray-200 rounded-lg text-gray-700 text-sm font-bold transition-colors flex-shrink-0"
              title="Search X"
            >𝕏</a>
          </div>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">Bluesky URL</label>
          <div className="flex gap-1.5">
            <input type="text" value={form.bluesky_url}
              onChange={e => setForm(f => ({ ...f, bluesky_url: e.target.value }))}
              placeholder="https://bsky.app/profile/handle.bsky.social"
              className="flex-1 rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
            <a
              href={`https://bsky.app/search?q=${encodeURIComponent((form.first_name + ' ' + form.last_name).trim())}`}
              target="_blank" rel="noopener noreferrer"
              className="px-2.5 py-2 bg-sky-50 hover:bg-sky-100 border border-sky-200 rounded-lg text-sky-600 text-sm transition-colors flex-shrink-0"
              title="Search Bluesky"
            >🦋</a>
          </div>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">LinkedIn URL</label>
          <div className="flex gap-1.5">
            <input type="text" value={form.linkedin_url}
              onChange={e => setForm(f => ({ ...f, linkedin_url: e.target.value }))}
              placeholder="https://linkedin.com/in/handle"
              className="flex-1 rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
            <a
              href={`https://www.linkedin.com/search/results/people/?keywords=${encodeURIComponent((form.first_name + ' ' + form.last_name).trim())}`}
              target="_blank" rel="noopener noreferrer"
              className="px-2.5 py-2 bg-blue-50 hover:bg-blue-100 border border-blue-200 rounded-lg text-blue-700 text-sm transition-colors flex-shrink-0"
              title="Search LinkedIn"
            >in</a>
          </div>
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
      {isEdit && episodes === null && (
        <p className="mt-5 border-t border-gray-100 pt-4 text-sm text-gray-400">Loading appearances…</p>
      )}
      {isEdit && episodes?.length > 0 && (
        <div className="mt-5 border-t border-gray-100 pt-4">
          <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-3">
            Episode Appearances ({episodes.reduce((s, p) => s + p.count, 0)} total)
          </p>
          <div className="space-y-2">
            {episodes.map(pod => (
              <div key={pod.podcast} className="rounded-lg border border-gray-100 overflow-hidden">
                <div
                  onClick={() => toggleShow(pod.podcast)}
                  className="w-full flex items-center gap-2 px-3 py-2 bg-gray-50 hover:bg-gray-100 text-left transition-colors cursor-pointer"
                >
                  {pod.cover_art_url && (
                    <img src={pod.cover_art_url} alt={pod.podcast}
                      className="w-6 h-6 rounded flex-shrink-0" />
                  )}
                  <a href={`/admin/shows?apple_podcast_id=${pod.apple_podcast_id}`}
                    onClick={e => e.stopPropagation()}
                    className="text-xs font-medium text-gray-700 hover:text-teal-600 flex-1 truncate">
                    {pod.podcast}
                  </a>
                  <span className="text-xs text-gray-400 flex-shrink-0">{pod.count} ep{pod.count !== 1 ? 's' : ''}</span>
                  <span className="text-gray-300 text-xs">{expandedShows[pod.podcast] ? '▲' : '▼'}</span>
                </div>
                {expandedShows[pod.podcast] && (
                  <div className="divide-y divide-gray-50">
                    {pod.episodes.map(ep => {
                      const busy = creditActionId === ep.episode_id;
                      return (
                        <div key={ep.episode_id} className="px-3 py-1.5 flex items-start gap-2">
                          <span className={`mt-0.5 w-1.5 h-1.5 rounded-full flex-shrink-0 ${ep.is_guest ? 'bg-blue-400' : 'bg-green-500'}`} />
                          <div className="min-w-0 flex-1">
                            <a href={`/admin/episodes?episode_id=${ep.episode_id}`}
                              className="text-xs text-gray-700 hover:text-teal-600 leading-snug block">
                              {ep.episode_title}
                            </a>
                            <p className="text-xs text-gray-400">{ep.published_date?.slice(0,10)} · {ep.data_source}</p>
                            {ep.snippet && (
                              <p className="text-xs text-gray-400 italic mt-0.5 leading-snug">
                                …{ep.snippet}…
                              </p>
                            )}
                          </div>
                          <div className="flex-shrink-0 flex items-center gap-1">
                            {ep.is_guest && (
                              <button
                                onClick={() => handleReclassifyToHost(ep.episode_id)}
                                disabled={busy}
                                title="Reclassify as Host (they're the show's host/co-host, not a guest)"
                                className="px-1.5 py-0.5 rounded text-xs font-medium bg-green-50 text-green-700 hover:bg-green-100 disabled:opacity-50"
                              >
                                → Host
                              </button>
                            )}
                            <button
                              onClick={() => handleRemoveCredit(ep.episode_id)}
                              disabled={busy}
                              title="Remove this credit (they're mentioned — a producer, cited article, footer thanks — not actually on this episode)"
                              className="px-1.5 py-0.5 rounded text-xs font-medium bg-red-50 text-red-700 hover:bg-red-100 disabled:opacity-50"
                            >
                              ✕
                            </button>
                          </div>
                        </div>
                      );
                    })}
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
  const [listError, setListError]   = useState(null);
  const [searchQ, setSearchQ]       = useState('');
  const [filter, setFilter]         = useState('all');
  const [sort, setSort]             = useState('appearances_desc');
  const [selected, setSelected]     = useState(null);  // person being edited
  const [deleteConfirm, setDeleteConfirm] = useState(null);
  const [count, setCount]           = useState(null);   // { total, allTotal }
  const [loadingMore, setLoadingMore] = useState(false);
  const searchRef = useRef(null);

  // offset > 0 appends the next page (Load more) instead of replacing the
  // list. Only the newest request may fill it, so a slow page can't land on
  // top of a list that has since been re-searched or re-filtered.
  const latestList = useRef(0);
  const fetchPeople = useCallback(async (q = '', f = 'all', s = 'appearances_desc', offset = 0) => {
    const token = ++latestList.current;
    const more = offset > 0;
    if (more) setLoadingMore(true); else setLoading(true);
    setListError(null);
    try {
      const params = new URLSearchParams({ q, filter: f, sort: s, limit: PAGE_SIZE, offset });
      const res = await adminFetch(`${API}/people?${params}`);
      if (!res.ok) throw new Error(`API error ${res.status}`);
      const data = await res.json();
      if (token !== latestList.current) return;
      const items = Array.isArray(data) ? data : (data.items || []);
      setPeople(prev => (more ? [...prev, ...items] : items));
      setCount({ total: data.total ?? items.length, allTotal: data.all_total });
    } catch (e) { if (token === latestList.current) setListError(e.message || 'Failed to load'); }
    finally { if (token === latestList.current) { setLoading(false); setLoadingMore(false); } }
  }, []);

  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => { fetchPeople(searchQ, filter, sort); }, [fetchPeople]);

  // Deep-link support: /admin/people?host_id=X auto-opens that person.
  // Fetched directly rather than found in the list, which is capped at
  // the top 100 by appearances and may not include the target person.
  useEffect(() => {
    const targetId = new URLSearchParams(window.location.search).get('host_id');
    if (!targetId) return;
    adminFetch(`${API}/people/${targetId}`)
      .then(r => r.ok ? r.json() : null)
      .then(person => { if (person) setSelected(person); })
      .catch(() => {});
  }, []);

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
      await adminFetch(`${API}/people/${host_id}`, { method: 'DELETE' });
      setDeleteConfirm(null);
      if (selected?.host_id === host_id) setSelected(null);
      // Remove in place rather than re-fetching, which would drop the pages
      // already loaded with Load more.
      setPeople(prev => prev.filter(p => p.host_id !== host_id));
      setCount(c => c && { total: Math.max(0, c.total - 1), allTotal: c.allTotal == null ? c.allTotal : Math.max(0, c.allTotal - 1) });
    } catch (e) { console.error(e); }
  };

  const handleSaved = (updatedPerson) => {
    // An edit updates its row in place (keeping pages loaded with Load more);
    // a new person needs the list re-fetched to find its place.
    if (updatedPerson && people.some(p => p.host_id === updatedPerson.host_id)) {
      setPeople(prev => prev.map(p => (p.host_id === updatedPerson.host_id ? { ...p, ...updatedPerson } : p)));
    } else {
      fetchPeople(searchQ, filter, sort);
    }
    // If the saved person is the one currently selected, update it so image refreshes
    if (updatedPerson && selected && updatedPerson.host_id === selected.host_id) {
      setSelected(prev => ({ ...prev, ...updatedPerson }));
    }
  };

  return (
    <div className="min-h-screen bg-gray-100 font-sans">
      <AdminHeader active="People" />

      <div className="max-w-7xl mx-auto px-4 pt-4 md:px-6 md:pt-6 -mb-4 md:-mb-2">
        <AdminSubTabs tabs={PEOPLE_TABS} active="people" />
      </div>

      <div className="flex flex-col md:flex-row gap-4 md:gap-6 p-4 md:p-6 max-w-7xl mx-auto">

        {/* LEFT — people list */}
        <div className="flex-1 min-w-0">

          {/* Search + filters */}
          <div className="bg-white rounded-2xl border border-gray-200 p-4 mb-4">
            <div className="relative mb-3">
            <input ref={searchRef} type="text" value={searchQ} onChange={handleSearch}
              placeholder="Search by name..."
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none pr-7" />
            {searchQ && (
              <button
                onClick={() => { setSearchQ(''); fetchPeople('', filter, sort); }}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600 text-lg leading-none"
              >×</button>
            )}
          </div>

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
          {count && <AdminListCount total={count.total} allTotal={count.allTotal} shown={people.length}
            noun="person" plural="people" />}
          <div className="bg-white rounded-2xl border border-gray-200 overflow-hidden overflow-y-auto max-h-[60vh] md:max-h-[calc(100vh-280px)]">
            {loading ? (
              <div className="py-12 text-center text-gray-400 text-sm">Loading...</div>
            ) : listError ? (
              <div className="py-12 flex flex-col items-center gap-3">
                <p className="text-sm font-semibold text-red-500">Couldn't load people</p>
                <p className="text-xs text-gray-500">{listError}</p>
                <button
                  className="px-3 py-1.5 bg-teal-600 text-white text-sm rounded hover:bg-teal-700"
                  onClick={() => fetchPeople(searchQ, filter, sort)}
                >
                  Retry
                </button>
              </div>
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
                        {(person.current_title || person.current_company) && (
                          <p className="text-xs text-gray-600 truncate flex items-center gap-1">
                            {person.current_org_id && <OrgLogo orgId={person.current_org_id} name={person.current_company} size={14} />}
                            <span className="truncate">
                              {person.current_title}
                              {person.current_title && person.current_company ? ' · ' : ''}
                              {person.current_company}
                            </span>
                          </p>
                        )}
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
                {count && people.length < count.total && (
                  <div className="p-3 text-center">
                    <button onClick={() => fetchPeople(searchQ, filter, sort, people.length)} disabled={loadingMore}
                      className="px-4 py-1.5 bg-gray-100 hover:bg-gray-200 disabled:opacity-50 text-gray-700 text-sm font-medium rounded-lg">
                      {loadingMore ? 'Loading…' : `Load more (${(count.total - people.length).toLocaleString()} remaining)`}
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        {/* RIGHT — add/edit panel */}
        <div className="w-full md:w-96 flex-shrink-0">
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
