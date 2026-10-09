import { useState, useEffect, useRef } from 'react';
import { API_BASE_URL } from '../config';
import OrgLogo from './OrgLogo';
import { Swatch } from './Topics';
import { personHref, orgHref, showHref, topicHref, avatarUrl, coverUrl, plural, imageUrl, imageFallback, publicFetch } from '../profileUtils';

// Name search across people, organisations and shows (/api/search).
export default function ProfileSearch({ className = '' }) {
  const [q, setQ] = useState('');
  const [results, setResults] = useState(null);
  const [open, setOpen] = useState(false);
  const boxRef = useRef(null);

  useEffect(() => {
    if (q.trim().length < 2) { setResults(null); return; }
    const t = setTimeout(() => {
      publicFetch(`${API_BASE_URL}/api/search?q=${encodeURIComponent(q.trim())}&limit=6`)
        .then(r => (r.ok ? r.json() : null)).then(setResults).catch(() => setResults(null));
    }, 200);
    return () => clearTimeout(t);
  }, [q]);

  useEffect(() => {
    const close = e => { if (boxRef.current && !boxRef.current.contains(e.target)) setOpen(false); };
    document.addEventListener('mousedown', close);
    return () => document.removeEventListener('mousedown', close);
  }, []);

  const empty = results && !results.people.length && !results.orgs.length && !results.shows.length && !results.topics?.length;
  const row = 'flex items-center gap-2 px-3 py-1.5 hover:bg-gray-50 text-sm text-gray-800';
  const heading = 'px-3 pt-2 pb-1 text-[11px] font-semibold uppercase tracking-wide text-gray-400';

  return (
    <div ref={boxRef} className={`relative ${className}`}>
      <input
        type="search" value={q} placeholder="Search people, orgs, shows, topics…"
        onChange={e => { setQ(e.target.value); setOpen(true); }} onFocus={() => setOpen(true)}
        className="w-full rounded-lg border border-gray-300 bg-white px-3 py-1.5 text-sm focus:border-teal-500 focus:outline-none"
      />
      {open && results && (
        <div className="absolute right-0 left-0 mt-1 z-20 max-h-96 overflow-y-auto rounded-lg border border-gray-200 bg-white shadow-lg text-left">
          {empty && <p className="px-3 py-2 text-sm text-gray-500">No matches</p>}
          {results.people.length > 0 && <p className={heading}>People</p>}
          {results.people.map(p => (
            <a key={`p${p.host_id}`} href={personHref(p.host_id, p.slug)} className={row}>
              <img src={p.profile_image_url ? imageUrl(p.profile_image_url, 64) : avatarUrl(p.name)} alt=""
                className="w-6 h-6 rounded-full object-cover" onError={imageFallback(p.profile_image_url, p.name)} />
              <span className="flex-1 truncate">{p.name}</span>
              <span className="text-xs text-gray-400">{plural(p.appearances, 'episode')}</span>
            </a>
          ))}
          {results.orgs.length > 0 && <p className={heading}>Organisations</p>}
          {results.orgs.map(o => (
            <a key={`o${o.org_id}`} href={orgHref(o.org_id, o.slug)} className={row}>
              <OrgLogo orgId={o.org_id} name={o.name} size={24} />
              <span className="flex-1 truncate">{o.name}</span>
              <span className="text-xs text-gray-400">{o.people > 0 ? plural(o.people, 'person', 'people') : `discussed on ${plural(o.discussed || 0, 'episode')}`}</span>
            </a>
          ))}
          {results.shows.length > 0 && <p className={heading}>Shows</p>}
          {results.shows.map(s => (
            <a key={`s${s.podcast_id}`} href={showHref(s.podcast_id, s.slug)} className={row}>
              <img src={s.cover_art_url ? coverUrl(s.cover_art_url, 80) : avatarUrl(s.title)} alt="" className="w-6 h-6 rounded object-cover" />
              <span className="flex-1 truncate">{s.title}</span>
            </a>
          ))}
          {results.topics?.length > 0 && <p className={heading}>Topics</p>}
          {results.topics?.map(t => (
            <a key={`t${t.tag_id}`} href={topicHref(t.tag_id, t.slug)} className={row}>
              <span className="w-6 h-6 flex items-center justify-center"><Swatch category={t.category} className="w-2.5 h-2.5" /></span>
              <span className="flex-1 truncate">{t.name}</span>
              <span className="text-xs text-gray-400">{plural(t.episodes, 'episode')}</span>
            </a>
          ))}
        </div>
      )}
    </div>
  );
}
