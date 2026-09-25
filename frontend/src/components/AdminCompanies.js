import React, { useState, useEffect, useCallback, useRef } from 'react';
import { API_BASE_URL } from '../config';
import { adminFetch } from '../adminAuth';
import AdminHeader from './AdminHeader';
import AdminSubTabs from './AdminSubTabs';
import AdminListCount from './AdminListCount';

// Company Admin. Organisations are created automatically from the company
// names extracted for guests (scraper/organizations.py sync); this page is
// where they get reviewed: merged when two spellings are one organisation,
// given a type and a parent, or marked as not an organisation at all.

const API = `${API_BASE_URL}/api/admin`;

const ORG_TYPES = ['company', 'nonprofit', 'government', 'academic', 'research',
                   'media', 'investor', 'association', 'other'];

const VIEWS = [
  { id: 'active',  label: 'All' },
  { id: 'untyped', label: 'No type yet' },
  { id: 'not_org', label: 'Not an organisation' },
];

const SORTS = [
  { id: 'people_desc', label: 'Most people' },
  { id: 'name_asc',    label: 'Name A–Z' },
  { id: 'newest',      label: 'Newest first' },
];

const REASONS = {
  acronym:  { label: 'Acronym',       bg: 'bg-purple-100 text-purple-700' },
  similar:  { label: 'Similar name',  bg: 'bg-blue-100 text-blue-700' },
  contains: { label: 'Name contains', bg: 'bg-amber-100 text-amber-700' },
};

async function call(url, options) {
  const res = await adminFetch(url, options);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    // A validation error (422) sends detail as a list of {loc, msg}.
    const detail = Array.isArray(data.detail)
      ? data.detail.map(d => `${(d.loc || []).slice(-1)[0]}: ${d.msg}`).join('; ')
      : data.detail;
    throw new Error(detail || `API error ${res.status}`);
  }
  return data;
}

const jsonBody = (method, body) => ({
  method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
});

// One colour per type so a scan of the list shows the mix at a glance.
const TYPE_COLORS = {
  company:     'bg-blue-100 text-blue-800',
  nonprofit:   'bg-green-100 text-green-800',
  government:  'bg-amber-100 text-amber-800',
  academic:    'bg-purple-100 text-purple-800',
  research:    'bg-teal-100 text-teal-800',
  media:       'bg-pink-100 text-pink-800',
  investor:    'bg-emerald-100 text-emerald-800',
  association: 'bg-orange-100 text-orange-800',
};

function TypeBadge({ type }) {
  if (!type) return null;
  return <span className={`text-xs px-1.5 py-0.5 rounded flex-shrink-0 ${TYPE_COLORS[type] || 'bg-gray-100 text-gray-600'}`}>{type}</span>;
}

// Search-as-you-type picker over companies, for choosing a parent or a merge target.
function CompanyPicker({ placeholder, excludeId, onPick }) {
  const [q, setQ] = useState('');
  const [results, setResults] = useState([]);

  useEffect(() => {
    if (q.trim().length < 2) { setResults([]); return undefined; }
    const t = setTimeout(() => {
      call(`${API}/companies?q=${encodeURIComponent(q.trim())}&limit=8`)
        .then(d => setResults((d.items || []).filter(o => o.org_id !== excludeId)))
        .catch(() => setResults([]));
    }, 250);
    return () => clearTimeout(t);
  }, [q, excludeId]);

  return (
    <div className="relative">
      <input type="text" value={q} onChange={e => setQ(e.target.value)} placeholder={placeholder}
        className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
      {results.length > 0 && (
        <div className="absolute z-10 mt-1 w-full bg-white border border-gray-200 rounded-lg shadow-lg max-h-56 overflow-y-auto">
          {results.map(o => (
            <button key={o.org_id} onClick={() => { onPick(o); setQ(''); setResults([]); }}
              className="w-full text-left px-3 py-2 text-sm hover:bg-gray-50 flex items-center justify-between gap-2">
              <span className="truncate" title={o.name}>{o.name}</span>
              <span className="text-xs text-gray-400 flex-shrink-0">{o.people} people</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

// Where each website came from (scraper/enrich.py); 'admin' = typed here.
const WEBSITE_SOURCE = {
  show_notes: 'from show notes', clearbit: 'from Clearbit', wikidata: 'from Wikidata',
  manual: 'added by hand', admin: 'set here',
};

// Facts found outside the episode text: links, where it's based, when founded.
function OrgFacts({ org }) {
  const links = [
    org.website_domain && { href: `https://${org.website_domain}`, label: org.website_domain,
      note: WEBSITE_SOURCE[org.website_source] },
    org.wikipedia_url && { href: org.wikipedia_url, label: 'Wikipedia' },
    org.linkedin_url && { href: org.linkedin_url, label: 'LinkedIn' },
    org.twitter_handle && { href: `https://x.com/${org.twitter_handle}`, label: `@${org.twitter_handle}` },
    org.bluesky_handle && { href: `https://bsky.app/profile/${org.bluesky_handle}`, label: org.bluesky_handle },
    org.wikidata_id && { href: `https://www.wikidata.org/wiki/${org.wikidata_id}`, label: 'Wikidata' },
  ].filter(Boolean);
  const place = [org.hq_city, org.country].filter(Boolean).join(', ');
  if (!links.length && !place && !org.founded_year) return null;
  return (
    <div>
      <p className="text-xs font-medium text-gray-500 mb-1">About</p>
      <div className="flex flex-wrap gap-x-3 gap-y-1 text-sm">
        {links.map(l => (
          <a key={l.href} href={l.href} target="_blank" rel="noopener noreferrer"
            className="text-blue-600 hover:underline truncate max-w-full" title={l.href}>
            {l.label}{l.note && <span className="text-xs text-gray-400"> · {l.note}</span>}
          </a>
        ))}
      </div>
      {(place || org.founded_year) && (
        <p className="text-xs text-gray-500 mt-1">
          {place}{place && org.founded_year ? ' · ' : ''}{org.founded_year ? `founded ${org.founded_year}` : ''}
        </p>
      )}
    </div>
  );
}

function CompanyPanel({ orgId, onChanged, onSelect, onClose }) {
  const [data, setData] = useState(null);
  const [form, setForm] = useState(null);
  const [includeSub, setIncludeSub] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [mergeTarget, setMergeTarget] = useState(null);
  // 'into': this company folds into the one picked (the usual case);
  // 'in': the one picked folds into this company.
  const [mergeDir, setMergeDir] = useState('into');

  // Only the newest request may fill the panel, so a slow response for a
  // row clicked earlier can't overwrite the one clicked since.
  const latest = useRef(0);
  const load = useCallback(() => {
    const token = ++latest.current;
    call(`${API}/companies/${orgId}?include_sub=${includeSub}`)
      .then(d => {
        if (token !== latest.current) return;
        setData(d);
        setForm({
          name: d.org.name, org_type: d.org.org_type || '', website_domain: d.org.website_domain || '',
          parent: d.org.parent_org_id ? { org_id: d.org.parent_org_id, name: d.org.parent_name } : null,
        });
      })
      .catch(e => { if (token === latest.current) setError(e.message); });
  }, [orgId, includeSub]);

  // A different company: show Loading at once rather than the previous one.
  useEffect(() => { setData(null); setForm(null); }, [orgId]);
  useEffect(() => { setError(''); setNotice(''); setMergeTarget(null); setMergeDir('into'); load(); }, [load]);

  const save = async (extra = {}) => {
    setBusy(true); setError(''); setNotice('');
    try {
      await call(`${API}/companies/${orgId}`, jsonBody('PUT', {
        name: form.name, org_type: form.org_type || null, website_domain: form.website_domain,
        parent_org_id: form.parent ? form.parent.org_id : 0, ...extra,
      }));
      setNotice('Saved');
      load(); onChanged?.();
    } catch (e) { setError(e.message); } finally { setBusy(false); }
  };

  const merge = async () => {
    if (!mergeTarget) return;
    setBusy(true); setError('');
    try {
      if (mergeDir === 'into') {
        // This company is merged away, so show the one it became part of.
        await call(`${API}/companies/${mergeTarget.org_id}/merge/${orgId}`, { method: 'POST' });
        setMergeTarget(null);
        onChanged?.();
        onSelect?.(mergeTarget.org_id);
      } else {
        const r = await call(`${API}/companies/${orgId}/merge/${mergeTarget.org_id}`, { method: 'POST' });
        setNotice(`Merged “${r.merged}” into this company`);
        setMergeTarget(null);
        load(); onChanged?.();
      }
    } catch (e) { setError(e.message); } finally { setBusy(false); }
  };

  // Act on one open merge suggestion from this panel.
  const decide = async (other, action) => {
    setBusy(true); setError(''); setNotice('');
    try {
      if (action === 'into') {
        await call(`${API}/companies/${other.org_id}/merge/${orgId}`, { method: 'POST' });
        onChanged?.(); onSelect?.(other.org_id);
        return;
      }
      if (action === 'in') {
        const r = await call(`${API}/companies/${orgId}/merge/${other.org_id}`, { method: 'POST' });
        setNotice(`Merged “${r.merged}” into this company`);
      } else {
        await call(`${API}/companies/not-same`, jsonBody('POST', { org_a: orgId, org_b: other.org_id }));
        setNotice(`Marked different from “${other.name}”`);
      }
      load(); onChanged?.();
    } catch (e) { setError(e.message); } finally { setBusy(false); }
  };

  if (!data || !form) {
    return <div className="bg-white rounded-2xl border border-gray-200 p-6 text-sm text-gray-400">
      {error || 'Loading…'}</div>;
  }
  const { org, aliases, children, people, suggestions = [] } = data;
  const currentCount = people.filter(p => p.is_current).length;

  return (
    <div className="bg-white rounded-2xl border border-gray-200 p-6 overflow-y-auto md:sticky md:top-6 md:max-h-[calc(100vh-80px)] space-y-5">
      <div className="flex items-center justify-between">
        <h2 className="text-base font-semibold text-gray-900">Edit Company</h2>
        <button onClick={onClose} className="text-xs text-gray-400 hover:text-gray-600">✕ Close</button>
      </div>
      {org.not_an_org && (
        <p className="text-xs bg-amber-50 text-amber-700 rounded-lg px-3 py-2">
          Marked as not an organisation — hidden from the list and never shown as anyone's company.
        </p>
      )}

      <div className="space-y-3">
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">Name</label>
          <input type="text" value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))}
            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
        </div>
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Type</label>
            <select value={form.org_type} onChange={e => setForm(f => ({ ...f, org_type: e.target.value }))}
              className="w-full rounded-lg border border-gray-300 px-2 py-2 text-sm focus:border-blue-500 focus:outline-none">
              <option value="">—</option>
              {ORG_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Website</label>
            <input type="text" value={form.website_domain} placeholder="example.com"
              onChange={e => setForm(f => ({ ...f, website_domain: e.target.value }))}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
          </div>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">Parent organisation</label>
          {form.parent ? (
            <div className="flex items-center justify-between bg-gray-50 rounded-lg px-3 py-2 text-sm">
              <button onClick={() => onSelect(form.parent.org_id)} title={form.parent.name} className="text-blue-600 hover:underline truncate">
                {form.parent.name}
              </button>
              <button onClick={() => setForm(f => ({ ...f, parent: null }))}
                className="text-xs text-gray-400 hover:text-red-600 ml-2">Remove</button>
            </div>
          ) : (
            <CompanyPicker placeholder="Search for a parent, e.g. Bloomberg" excludeId={orgId}
              onPick={o => setForm(f => ({ ...f, parent: o }))} />
          )}
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          <button onClick={() => save()} disabled={busy}
            className="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">
            Save
          </button>
          <button onClick={() => save({ not_an_org: !org.not_an_org })} disabled={busy}
            className="px-3 py-1.5 text-sm rounded-lg border border-gray-300 text-gray-600 hover:bg-gray-50 disabled:opacity-50">
            {org.not_an_org ? 'It is an organisation' : 'Not an organisation'}
          </button>
          {notice && <span className="text-xs text-green-600">{notice}</span>}
        </div>
        {error && <p className="text-xs text-red-500">{error}</p>}
      </div>

      <OrgFacts org={org} />

      {suggestions.length > 0 && (
        <div className="bg-amber-50 rounded-lg p-3">
          <p className="text-xs font-medium text-amber-800 mb-2">
            Possible duplicates ({suggestions.length}) — open merge suggestions
          </p>
          <ul className="space-y-2">
            {suggestions.map(o => (
              <li key={o.org_id} className="text-sm">
                <div className="flex items-center gap-2 min-w-0">
                  <button onClick={() => onSelect(o.org_id)} title={o.name} className="text-blue-600 hover:underline truncate">{o.name}</button>
                  <TypeBadge type={o.org_type} />
                  <span className="text-xs text-gray-400 flex-shrink-0">{o.people} {o.people === 1 ? 'person' : 'people'} · {o.reason}</span>
                </div>
                <div className="flex gap-1.5 mt-1">
                  {[['into', 'Merge this into it'], ['in', 'Merge it into this'], ['different', 'Different']].map(([a, label]) => (
                    <button key={a} onClick={() => decide(o, a)} disabled={busy}
                      className="text-xs px-2 py-0.5 rounded border border-amber-300 text-amber-800 hover:bg-white disabled:opacity-50">
                      {label}
                    </button>
                  ))}
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}

      <div>
        <p className="text-xs font-medium text-gray-500 mb-1">Spellings ({aliases.length})</p>
        <p className="text-xs text-gray-400 mb-2">Every way this organisation was written in episode text.</p>
        <div className="flex flex-wrap gap-1.5">
          {aliases.map(a => (
            <span key={a.alias_id} className="text-xs bg-gray-100 text-gray-700 rounded-full px-2.5 py-1"
              title={`${a.roles} role${a.roles !== 1 ? 's' : ''} · ${a.source}`}>
              {a.alias_name} <span className="text-gray-400">{a.roles}</span>
            </span>
          ))}
        </div>
      </div>

      <div>
        <div className="flex items-center gap-2 mb-1">
          <p className="text-xs font-medium text-gray-500">Merge</p>
          {[['into', 'this into another company'], ['in', 'another company into this']].map(([id, label]) => (
            <button key={id} onClick={() => { setMergeDir(id); setMergeTarget(null); }}
              className={`text-xs px-2 py-0.5 rounded ${mergeDir === id ? 'bg-gray-900 text-white' : 'text-gray-600 hover:bg-gray-100'}`}>
              {label}
            </button>
          ))}
        </div>
        {mergeTarget ? (
          <div className="bg-red-50 rounded-lg p-3 text-sm space-y-2">
            {mergeDir === 'into' ? (
              <p className="text-gray-700">
                Fold <strong>{org.name}</strong> ({people.length} people) into <strong>{mergeTarget.name}</strong>?
                This company's spellings become {mergeTarget.name}'s, and this record goes away.
              </p>
            ) : (
              <p className="text-gray-700">
                Fold <strong>{mergeTarget.name}</strong> ({mergeTarget.people} people) into <strong>{org.name}</strong>?
                Its spellings become this company's.
              </p>
            )}
            <div className="flex gap-2">
              <button onClick={merge} disabled={busy}
                className="px-3 py-1.5 text-sm rounded-lg bg-red-600 text-white hover:bg-red-700 disabled:opacity-50">Merge</button>
              <button onClick={() => setMergeTarget(null)}
                className="px-3 py-1.5 text-sm rounded-lg border border-gray-300 text-gray-600 hover:bg-white">Cancel</button>
            </div>
          </div>
        ) : (
          <CompanyPicker placeholder={mergeDir === 'into' ? 'Search for the company to merge into…' : 'Search for a duplicate…'}
            excludeId={orgId} onPick={setMergeTarget} />
        )}
      </div>

      {children.length > 0 && (
        <div>
          <p className="text-xs font-medium text-gray-500 mb-1">Sub-organisations ({children.length})</p>
          <ul className="space-y-1">
            {children.map(c => (
              <li key={c.org_id} className="text-sm flex items-center justify-between">
                <button onClick={() => onSelect(c.org_id)} title={c.name} className="text-blue-600 hover:underline truncate">{c.name}</button>
                <span className="text-xs text-gray-400">{c.people} people</span>
              </li>
            ))}
          </ul>
        </div>
      )}

      <div>
        <div className="flex items-center justify-between mb-1">
          <p className="text-xs font-medium text-gray-500">
            People ({people.length}{currentCount ? `, ${currentCount} current` : ''})
          </p>
          {children.length > 0 && (
            <label className="text-xs text-gray-500 flex items-center gap-1">
              <input type="checkbox" checked={includeSub} onChange={e => setIncludeSub(e.target.checked)} />
              include sub-organisations
            </label>
          )}
        </div>
        <ul className="space-y-2">
          {people.map(p => (
            <li key={p.host_id} className="text-xs">
              <div className="flex items-center gap-2">
                <a href={`/admin/people?host_id=${p.host_id}`} className="text-sm text-gray-900 hover:underline">{p.name}</a>
                {p.is_current && <span className="px-1.5 py-0.5 rounded bg-green-100 text-green-700">current</span>}
              </div>
              {p.roles.slice(0, 3).map((r, i) => (
                <p key={i} className={r.is_former ? 'text-gray-400' : 'text-gray-600'}>
                  {r.title || '—'}{r.is_former ? ' (former)' : ''}
                  <span className="text-gray-400"> · {r.company} · {r.podcast_title}{r.published_date ? `, ${r.published_date}` : ''}</span>
                </p>
              ))}
            </li>
          ))}
          {people.length === 0 && <li className="text-xs text-gray-400">No roles on record.</li>}
        </ul>
      </div>
    </div>
  );
}

function SuggestionCard({ s, onAction, onSkip }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const reason = REASONS[s.reason] || REASONS.similar;

  // droppedId: a company that no longer exists after this action, so every
  // other card naming it is stale too.
  const act = async (fn, droppedId = null) => {
    setBusy(true); setError('');
    try {
      await fn();
      onAction(s, droppedId);
    } catch (e) {
      // Already merged away (by an earlier card or another tab): this card is
      // stale, not an error to act on.
      if (/not found/i.test(e.message)) { onAction(s, null, true); return; }
      setError(e.message); setBusy(false);
    }
  };
  const merge = (keep, drop) => act(
    () => call(`${API}/companies/${keep.org_id}/merge/${drop.org_id}`, { method: 'POST' }), drop.org_id);
  const parent = (par, child) => act(() => call(`${API}/companies/${child.org_id}`, jsonBody('PUT', { parent_org_id: par.org_id })));
  const different = () => act(() => call(`${API}/companies/not-same`, jsonBody('POST', { org_a: s.a.org_id, org_b: s.b.org_id })));

  const side = (o) => (
    <div className="min-w-0">
      <p className="text-sm font-medium text-gray-900 truncate" title={o.name}>{o.name}</p>
      <p className="text-xs text-gray-400">{o.people} people{o.org_type ? ` · ${o.org_type}` : ''}</p>
      {o.alias_names?.length > 1 && (
        <p className="text-xs text-gray-400 truncate" title={o.alias_names.filter(n => n !== o.name).join(', ')}>also: {o.alias_names.filter(n => n !== o.name).join(', ')}</p>
      )}
    </div>
  );

  const btn = 'px-2.5 py-1 text-xs rounded-lg border border-gray-300 text-gray-700 hover:bg-gray-50 disabled:opacity-50';
  return (
    <div className="bg-white rounded-2xl border border-gray-200 p-4 space-y-3">
      <div className="flex items-center gap-2">
        <span className={`text-xs px-1.5 py-0.5 rounded ${reason.bg}`}>{reason.label}</span>
        {s.reason === 'similar' && <span className="text-xs text-gray-400">similarity {s.score}</span>}
      </div>
      <div className="grid grid-cols-2 gap-4">{side(s.a)}{side(s.b)}</div>
      <div className="flex flex-wrap gap-1.5">
        <button disabled={busy} onClick={() => merge(s.a, s.b)} className={btn}>Same — keep “{s.a.name}”</button>
        <button disabled={busy} onClick={() => merge(s.b, s.a)} className={btn}>Same — keep “{s.b.name}”</button>
        <button disabled={busy} onClick={() => parent(s.a, s.b)} className={btn}>“{s.b.name}” is part of “{s.a.name}”</button>
        <button disabled={busy} onClick={() => parent(s.b, s.a)} className={btn}>“{s.a.name}” is part of “{s.b.name}”</button>
        <button disabled={busy} onClick={different} className={btn}>Different</button>
        <button disabled={busy} onClick={() => onSkip(s)} className="px-2.5 py-1 text-xs text-gray-400 hover:text-gray-600">Skip</button>
      </div>
      {error && <p className="text-xs text-red-500">{error}</p>}
    </div>
  );
}

const PAGE = 40;

function timeAgo(iso) {
  if (!iso) return 'never';
  const mins = Math.round((Date.now() - new Date(iso + (iso.endsWith('Z') ? '' : 'Z')).getTime()) / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins} min ago`;
  const hrs = Math.round(mins / 60);
  return hrs < 48 ? `${hrs} h ago` : `${Math.round(hrs / 24)} days ago`;
}

// The queue is stored server-side (company_merge_suggestions) and rebuilt
// after each extraction run or by Recompute, so this only reads a page of it.
function Suggestions({ onChanged }) {
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [computedAt, setComputedAt] = useState(null);
  const [shown, setShown] = useState(PAGE);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [hidden, setHidden] = useState(new Set());
  const [gone, setGone] = useState(new Set());   // companies merged away this session
  const [skipped, setSkipped] = useState(new Set());   // survives reloads until Refresh

  // quiet: refresh in the background without replacing the list by "Loading…".
  const load = useCallback((quiet = false, count = PAGE) => {
    if (!quiet) setLoading(true);
    call(`${API}/companies-suggestions?limit=${count}`)
      .then(d => {
        setItems(d.items || []); setTotal(d.total || 0); setComputedAt(d.computed_at);
        setError(''); setHidden(new Set());
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);
  useEffect(() => { load(false, shown); }, [load]);   // eslint-disable-line react-hooks/exhaustive-deps

  const recompute = async () => {
    setNotice('');
    try {
      await call(`${API}/companies-suggestions/refresh`, { method: 'POST' });
      setNotice('Recomputing — this takes a minute or two. Refresh after that to see the new queue.');
    } catch (e) { setError(e.message); }
  };

  const key = (s) => `${s.org_a}-${s.org_b}`;
  const done = (s, droppedId, stale = false) => {
    // Hide this card, and every card naming a company that was just merged
    // away — those would only fail with "not found". Then re-read the
    // stored queue (a decision removes its own row server-side).
    setHidden(prev => new Set(prev).add(key(s)));
    if (droppedId) setGone(prev => new Set(prev).add(droppedId));
    if (!stale) onChanged?.();
    load(true, shown);
  };
  const skip = (s) => setSkipped(prev => new Set(prev).add(key(s)));
  const more = () => { const n = shown + PAGE; setShown(n); load(true, n); };
  const visible = items.filter(s => !hidden.has(key(s)) && !skipped.has(key(s))
                                 && !gone.has(s.org_a) && !gone.has(s.org_b));

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div>
          <AdminListCount total={total} shown={items.length} noun="possible duplicate" className="mb-0" />
          <p className="text-xs text-gray-400">Biggest first · computed {timeAgo(computedAt)}.</p>
        </div>
        <div className="flex gap-3">
          <button onClick={recompute} className="text-xs text-gray-500 hover:underline">Recompute</button>
          <button onClick={() => { setSkipped(new Set()); load(false, shown); }}
            className="text-xs text-blue-600 hover:underline">Refresh</button>
        </div>
      </div>
      {notice && <p className="text-xs text-green-700 bg-green-50 rounded-lg px-3 py-2">{notice}</p>}
      {loading ? <p className="text-sm text-gray-400">Loading…</p>
        : error ? <p className="text-sm text-red-500">{error}</p>
        : visible.length === 0 ? <p className="text-sm text-gray-400">Nothing left here — Refresh or Load more.</p>
        : visible.map(s => <SuggestionCard key={key(s)} s={s} onAction={done} onSkip={skip} />)}
      {!loading && !error && items.length < total && (
        <button onClick={more} className="w-full py-2 text-sm text-blue-600 hover:bg-white rounded-lg">
          Load more ({Math.min(PAGE, total - items.length)} of {total - items.length} remaining)
        </button>
      )}
    </div>
  );
}

// Each tab has its own address, so a refresh or a shared link lands on it.
const TAB_PATHS = { companies: '/admin/companies', suggestions: '/admin/companies/suggestions' };
const tabFromPath = () =>
  window.location.pathname.startsWith(TAB_PATHS.suggestions) ? 'suggestions' : 'companies';

export default function AdminCompanies() {
  const [tab, setTabState] = useState(tabFromPath);
  const setTab = (t) => {
    setTabState(t);
    if (window.location.pathname !== TAB_PATHS[t]) window.history.pushState(null, '', TAB_PATHS[t]);
  };
  // Browser back/forward between the two tabs.
  useEffect(() => {
    const onPop = () => setTabState(tabFromPath());
    window.addEventListener('popstate', onPop);
    return () => window.removeEventListener('popstate', onPop);
  }, []);
  const [items, setItems] = useState([]);
  const [count, setCount] = useState(null);   // { total, allTotal }
  const [q, setQ] = useState('');
  const [view, setView] = useState('active');
  const [orgType, setOrgType] = useState('');
  const [sort, setSort] = useState('people_desc');
  const [selected, setSelected] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const load = useCallback(() => {
    setLoading(true);
    const params = new URLSearchParams({ q, view, org_type: orgType, sort, limit: '300' });
    call(`${API}/companies?${params}`)
      .then(d => { setItems(d.items || []); setCount({ total: d.total, allTotal: d.all_total }); setError(''); })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, [q, view, orgType, sort]);

  useEffect(() => {
    const t = setTimeout(load, 250);
    return () => clearTimeout(t);
  }, [load]);


  return (
    <div className="min-h-screen bg-gray-100 font-sans">
      <AdminHeader active="Companies" />
      <div className="max-w-7xl mx-auto p-4 sm:p-6">
        <AdminSubTabs active={tab} tabs={[
          { id: 'companies',   label: 'Companies',         onClick: () => setTab('companies') },
          { id: 'suggestions', label: 'Merge suggestions', onClick: () => setTab('suggestions') },
        ]} />

        {tab === 'suggestions' ? (
          <div className="max-w-3xl"><Suggestions onChanged={load} /></div>
        ) : (
          <div className="flex flex-col md:flex-row gap-6">
            <div className="flex-1 min-w-0">
              <div className="bg-white rounded-2xl border border-gray-200 p-4 mb-3 space-y-3">
                <input type="text" value={q} onChange={e => setQ(e.target.value)} placeholder="Search companies or spellings…"
                  className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none" />
                <div className="flex gap-2 flex-wrap">
                  {VIEWS.map(v => (
                    <button key={v.id} onClick={() => setView(v.id)}
                      className={`px-3 py-1 rounded-full text-xs font-medium ${view === v.id
                        ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}>
                      {v.label}
                    </button>
                  ))}
                </div>
                <div className="flex items-center gap-3 flex-wrap text-xs">
                  <label className="flex items-center gap-1 text-gray-400">Type:
                    <select value={orgType} onChange={e => setOrgType(e.target.value)}
                      className="border border-gray-200 rounded px-2 py-1 text-gray-700">
                      <option value="">Any</option>
                      {ORG_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                    </select>
                  </label>
                  <label className="flex items-center gap-1 text-gray-400">Sort:
                    <select value={sort} onChange={e => setSort(e.target.value)}
                      className="border border-gray-200 rounded px-2 py-1 text-gray-700">
                      {SORTS.map(s => <option key={s.id} value={s.id}>{s.label}</option>)}
                    </select>
                  </label>
                </div>
              </div>

              {count && <AdminListCount total={count.total} allTotal={count.allTotal} shown={items.length}
                noun="company" plural="companies" />}
              <div className="bg-white rounded-2xl border border-gray-200 overflow-hidden overflow-y-auto max-h-[60vh] md:max-h-[calc(100vh-300px)]">
                {loading && items.length === 0 ? <div className="py-12 text-center text-gray-400 text-sm">Loading…</div>
                  : error ? <div className="py-12 text-center text-red-500 text-sm">{error}</div>
                  : items.length === 0 ? <div className="py-12 text-center text-gray-400 text-sm">No companies found</div>
                  : (
                    <div className="divide-y divide-gray-100">
                      {items.map(o => (
                        <div key={o.org_id} onClick={() => setSelected(o.org_id === selected ? null : o.org_id)}
                          className={`px-4 py-3 cursor-pointer hover:bg-gray-50 ${o.org_id === selected ? 'bg-blue-50 border-l-2 border-blue-500' : ''}`}>
                          <div className="flex items-center gap-2">
                            <p className="text-sm font-medium text-gray-900 truncate" title={o.name}>{o.name}</p>
                            <TypeBadge type={o.org_type} />
                          </div>
                          <p className="text-xs text-gray-400">
                            {o.people} {o.people === 1 ? 'person' : 'people'}
                            {o.alias_count > 1 && ` · ${o.alias_count} spellings`}
                            {o.parent_name && ` · part of ${o.parent_name}`}
                            {o.child_count > 0 && ` · ${o.child_count} sub-org${o.child_count !== 1 ? 's' : ''}`}
                            {o.suggestion_count > 0 && (
                              <span className="text-amber-600">
                                {` · ${o.suggestion_count} merge suggestion${o.suggestion_count !== 1 ? 's' : ''}`}
                              </span>
                            )}
                            {o.website_domain && ` · ${o.website_domain}`}
                          </p>
                        </div>
                      ))}
                    </div>
                  )}
              </div>
            </div>

            <div className="w-full md:w-[28rem] flex-shrink-0">
              {selected ? (
                <CompanyPanel orgId={selected} onChanged={load} onSelect={setSelected} onClose={() => setSelected(null)} />
              ) : (
                <div className="bg-white rounded-2xl border border-gray-200 p-6 text-sm text-gray-400">
                  Pick a company to edit its name, type, website and parent, merge duplicates into it, and see its people.
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
