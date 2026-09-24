import React, { useState, useEffect, useCallback } from 'react';
import { API_BASE_URL } from '../config';
import { adminFetch } from '../adminAuth';
import AdminHeader from './AdminHeader';

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
  if (!res.ok) throw new Error(data.detail || `API error ${res.status}`);
  return data;
}

const jsonBody = (method, body) => ({
  method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
});

function TypeBadge({ type }) {
  if (!type) return null;
  return <span className="text-xs px-1.5 py-0.5 rounded bg-gray-100 text-gray-600 flex-shrink-0">{type}</span>;
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
              <span className="truncate">{o.name}</span>
              <span className="text-xs text-gray-400 flex-shrink-0">{o.people} people</span>
            </button>
          ))}
        </div>
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

  const load = useCallback(() => {
    call(`${API}/companies/${orgId}?include_sub=${includeSub}`)
      .then(d => {
        setData(d);
        setForm({
          name: d.org.name, org_type: d.org.org_type || '', website_domain: d.org.website_domain || '',
          parent: d.org.parent_org_id ? { org_id: d.org.parent_org_id, name: d.org.parent_name } : null,
        });
      })
      .catch(e => setError(e.message));
  }, [orgId, includeSub]);

  useEffect(() => { setError(''); setNotice(''); setMergeTarget(null); load(); }, [load]);

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

  const mergeIn = async () => {
    if (!mergeTarget) return;
    setBusy(true); setError('');
    try {
      const r = await call(`${API}/companies/${orgId}/merge/${mergeTarget.org_id}`, { method: 'POST' });
      setNotice(`Merged “${r.merged}” into this company`);
      setMergeTarget(null);
      load(); onChanged?.();
    } catch (e) { setError(e.message); } finally { setBusy(false); }
  };

  if (!data || !form) {
    return <div className="bg-white rounded-2xl border border-gray-200 p-6 text-sm text-gray-400">
      {error || 'Loading…'}</div>;
  }
  const { org, aliases, children, people } = data;
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
              <button onClick={() => onSelect(form.parent.org_id)} className="text-blue-600 hover:underline truncate">
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
        <p className="text-xs font-medium text-gray-500 mb-1">Merge another company into this one</p>
        {mergeTarget ? (
          <div className="bg-red-50 rounded-lg p-3 text-sm space-y-2">
            <p className="text-gray-700">
              Fold <strong>{mergeTarget.name}</strong> ({mergeTarget.people} people) into <strong>{org.name}</strong>?
              Its spellings become this company's.
            </p>
            <div className="flex gap-2">
              <button onClick={mergeIn} disabled={busy}
                className="px-3 py-1.5 text-sm rounded-lg bg-red-600 text-white hover:bg-red-700 disabled:opacity-50">Merge</button>
              <button onClick={() => setMergeTarget(null)}
                className="px-3 py-1.5 text-sm rounded-lg border border-gray-300 text-gray-600 hover:bg-white">Cancel</button>
            </div>
          </div>
        ) : (
          <CompanyPicker placeholder="Search for a duplicate…" excludeId={orgId} onPick={setMergeTarget} />
        )}
      </div>

      {children.length > 0 && (
        <div>
          <p className="text-xs font-medium text-gray-500 mb-1">Sub-organisations ({children.length})</p>
          <ul className="space-y-1">
            {children.map(c => (
              <li key={c.org_id} className="text-sm flex items-center justify-between">
                <button onClick={() => onSelect(c.org_id)} className="text-blue-600 hover:underline truncate">{c.name}</button>
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

function SuggestionCard({ s, onAction }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const reason = REASONS[s.reason] || REASONS.similar;

  const act = async (fn) => {
    setBusy(true); setError('');
    try { await fn(); onAction(s); } catch (e) { setError(e.message); setBusy(false); }
  };
  const merge = (keep, drop) => act(() => call(`${API}/companies/${keep.org_id}/merge/${drop.org_id}`, { method: 'POST' }));
  const parent = (par, child) => act(() => call(`${API}/companies/${child.org_id}`, jsonBody('PUT', { parent_org_id: par.org_id })));
  const different = () => act(() => call(`${API}/companies/not-same`, jsonBody('POST', { org_a: s.a.org_id, org_b: s.b.org_id })));

  const side = (o) => (
    <div className="min-w-0">
      <p className="text-sm font-medium text-gray-900 truncate">{o.name}</p>
      <p className="text-xs text-gray-400">{o.people} people{o.org_type ? ` · ${o.org_type}` : ''}</p>
      {o.alias_names?.length > 1 && (
        <p className="text-xs text-gray-400 truncate">also: {o.alias_names.filter(n => n !== o.name).join(', ')}</p>
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
        <button disabled={busy} onClick={() => onAction(s)} className="px-2.5 py-1 text-xs text-gray-400 hover:text-gray-600">Skip</button>
      </div>
      {error && <p className="text-xs text-red-500">{error}</p>}
    </div>
  );
}

function Suggestions({ onChanged }) {
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [hidden, setHidden] = useState(new Set());

  const load = useCallback(() => {
    setLoading(true);
    call(`${API}/companies-suggestions?limit=40`)
      .then(d => { setItems(d.items || []); setTotal(d.total || 0); setError(''); })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);
  useEffect(() => { load(); }, [load]);

  const key = (s) => `${s.org_a}-${s.org_b}`;
  const done = (s) => {
    // A merge or parent change can invalidate other cards; drop this one now
    // and refresh the queue in the background.
    setHidden(prev => new Set(prev).add(key(s)));
    onChanged?.();
  };
  const visible = items.filter(s => !hidden.has(key(s)));

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <p className="text-sm text-gray-500">
          {total} possible duplicate{total !== 1 ? 's' : ''}, biggest first. Merged spellings re-link every role at once.
        </p>
        <button onClick={() => { setHidden(new Set()); load(); }} className="text-xs text-blue-600 hover:underline">Refresh</button>
      </div>
      {loading ? <p className="text-sm text-gray-400">Loading…</p>
        : error ? <p className="text-sm text-red-500">{error}</p>
        : visible.length === 0 ? <p className="text-sm text-gray-400">Nothing left in this page of suggestions — Refresh for more.</p>
        : visible.map(s => <SuggestionCard key={key(s)} s={s} onAction={done} />)}
    </div>
  );
}

export default function AdminCompanies() {
  const [tab, setTab] = useState('companies');
  const [items, setItems] = useState([]);
  const [totals, setTotals] = useState(null);
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
      .then(d => { setItems(d.items || []); setTotals(d.totals); setError(''); })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, [q, view, orgType, sort]);

  useEffect(() => {
    const t = setTimeout(load, 250);
    return () => clearTimeout(t);
  }, [load]);

  const tabBtn = (id, label) => (
    <button onClick={() => setTab(id)}
      className={`px-3 py-1.5 rounded-lg text-sm ${tab === id ? 'bg-gray-900 text-white' : 'text-gray-600 hover:bg-gray-200'}`}>
      {label}
    </button>
  );

  return (
    <div className="min-h-screen bg-gray-100 font-sans">
      <AdminHeader active="Companies"
        right={totals && <span className="text-sm text-gray-400">{totals.active} companies · {totals.untyped} untyped</span>} />
      <div className="max-w-7xl mx-auto p-4 sm:p-6">
        <div className="flex gap-2 mb-4">
          {tabBtn('companies', 'Companies')}
          {tabBtn('suggestions', 'Merge suggestions')}
        </div>

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
                            <p className="text-sm font-medium text-gray-900 truncate">{o.name}</p>
                            <TypeBadge type={o.org_type} />
                          </div>
                          <p className="text-xs text-gray-400">
                            {o.people} {o.people === 1 ? 'person' : 'people'}
                            {o.alias_count > 1 && ` · ${o.alias_count} spellings`}
                            {o.parent_name && ` · part of ${o.parent_name}`}
                            {o.child_count > 0 && ` · ${o.child_count} sub-org${o.child_count !== 1 ? 's' : ''}`}
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
