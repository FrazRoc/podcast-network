import { useState, useEffect, useRef } from 'react';
import { API_BASE_URL } from '../config';
import { publicFetch } from '../profileUtils';

// Shared pieces of the /people, /orgs and /shows directories.

const readParams = (defaults) => {
  let search;
  try { search = new URLSearchParams(window.location.search); } catch { search = new URLSearchParams(); }
  return Object.fromEntries(Object.entries(defaults).map(([k, v]) => [k, search.get(k) ?? v]));
};

// Filter state mirrored into the address bar (defaults left out), so a
// filtered view can be linked and survives the back button.
export function useUrlParams(defaults) {
  const [params, setParams] = useState(() => readParams(defaults));
  useEffect(() => {
    const search = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => { if (v && v !== defaults[k]) search.set(k, v); });
    const qs = search.toString();
    try { window.history.replaceState(null, '', `${window.location.pathname}${qs ? `?${qs}` : ''}`); } catch { /* sandboxed */ }
  // defaults is a fresh object each render; only the values matter.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(params)]);
  return [params, (patch) => setParams(p => ({ ...p, ...patch }))];
}

// A paged /api/directory/<kind> list: reloads from the top when the filters
// change, appends on loadMore().
export function useDirectory(kind, params, pageSize = 50) {
  const [state, setState] = useState({ rows: [], total: 0, data: null, loading: true, error: false });
  const request = useRef(0);
  const query = (offset) => {
    const search = new URLSearchParams({ ...params, offset, limit: pageSize });
    return publicFetch(`${API_BASE_URL}/api/directory/${kind}?${search}`).then(r => (r.ok ? r.json() : Promise.reject(r)));
  };
  useEffect(() => {
    const id = ++request.current;
    setState(s => ({ ...s, loading: true, error: false }));
    query(0)
      .then(data => { if (id === request.current) setState({ rows: data.rows, total: data.total, data, loading: false, error: false }); })
      .catch(() => { if (id === request.current) setState(s => ({ ...s, loading: false, error: true })); });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [kind, JSON.stringify(params)]);
  const loadMore = () => {
    const id = request.current;
    setState(s => ({ ...s, loading: true }));
    query(state.rows.length)
      .then(data => { if (id === request.current) setState(s => ({ ...s, rows: [...s.rows, ...data.rows], loading: false })); })
      .catch(() => { if (id === request.current) setState(s => ({ ...s, loading: false, error: true })); });
  };
  return { ...state, loadMore, hasMore: state.rows.length < state.total };
}

// A search box that reports its text after a short pause.
export function SearchBox({ value, onChange, placeholder }) {
  const [text, setText] = useState(value);
  useEffect(() => {
    if (text === value) return undefined;
    const t = setTimeout(() => onChange(text.trim()), 250);
    return () => clearTimeout(t);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [text]);
  return (
    <input type="search" value={text} placeholder={placeholder} onChange={e => setText(e.target.value)}
      className="flex-1 min-w-0 rounded-lg border border-gray-300 bg-white px-3 py-1.5 text-sm focus:border-teal-500 focus:outline-none" />
  );
}

export function SortSelect({ value, options, onChange }) {
  return (
    <select value={value} onChange={e => onChange(e.target.value)} aria-label="Sort"
      className="rounded-lg border border-gray-300 bg-white px-2 py-1.5 text-sm text-gray-700 focus:border-teal-500 focus:outline-none">
      {options.map(([v, label]) => <option key={v} value={v}>{label}</option>)}
    </select>
  );
}

// "All" plus one chip per group, each with a colour swatch and its count.
export function FilterChips({ chips, value, onChange, labels = {}, colors = {} }) {
  const chip = (active) => `inline-flex items-center gap-1.5 whitespace-nowrap rounded-full border px-2.5 py-1 text-xs ${
    active ? 'border-teal-600 bg-teal-50 text-teal-800' : 'border-gray-200 bg-white text-gray-600 hover:border-gray-300'}`;
  return (
    // One scrollable row on a phone, wrapped on wider screens.
    <div className="flex gap-1.5 overflow-x-auto -mx-4 px-4 pb-1 sm:mx-0 sm:px-0 sm:pb-0 sm:flex-wrap">
      <button onClick={() => onChange('')} className={chip(!value)}>All</button>
      {chips.map(c => (
        <button key={c.kind} onClick={() => onChange(value === c.kind ? '' : c.kind)} className={chip(value === c.kind)}>
          {colors[c.kind] && <span className="w-2 h-2 rounded-sm" style={{ background: colors[c.kind] }} />}
          {labels[c.kind] || c.label}
          <span className="text-gray-400">{c.count.toLocaleString()}</span>
        </button>
      ))}
    </div>
  );
}

export function DirectoryHeading({ title, blurb }) {
  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900">{title}</h1>
      {blurb && <p className="mt-1 text-sm text-gray-600">{blurb}</p>}
    </div>
  );
}

// The list card: rows, a status line, and "Load more".
export function DirectoryList({ state, render, noun }) {
  const { rows, total, loading, error, hasMore, loadMore } = state;
  return (
    <section className="bg-white rounded-2xl border border-gray-200 px-4 sm:px-6 py-2">
      <p className="py-2 text-xs text-gray-400 border-b border-gray-100">
        {error ? 'Couldn\'t load this list. Please try again in a moment.'
          : loading && !rows.length ? 'Loading…' : `${total.toLocaleString()} ${noun}`}
      </p>
      <ul className="divide-y divide-gray-100">{rows.map(render)}</ul>
      {!loading && !error && total === 0 && <p className="py-8 text-center text-sm text-gray-500">No matches.</p>}
      {hasMore && (
        <div className="py-3 text-center">
          <button onClick={loadMore} disabled={loading}
            className="rounded-lg border border-gray-300 px-4 py-1.5 text-sm text-gray-700 hover:border-teal-500 disabled:opacity-50">
            {loading ? 'Loading…' : `Load more (${(total - rows.length).toLocaleString()} left)`}
          </button>
        </div>
      )}
    </section>
  );
}
