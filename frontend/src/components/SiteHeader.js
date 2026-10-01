import ProfileSearch from './ProfileSearch';
import Credits from './Credits';
import { getAdminPassword } from '../adminAuth';

const NAV = [
  ['/', 'Network'],
  ['/people', 'People'],
  ['/orgs', 'Organisations', 'Orgs'],
  ['/shows', 'Shows'],
  ['/stats', 'Stats'],
];

const isActive = (href, path) => (href === '/' ? path === '/' : path === href || path.startsWith(`${href}/`));

const loggedIn = () => { try { return !!getAdminPassword(); } catch { return false; } };

// The one top bar on every public page. On a phone the links drop to their
// own scrollable row under the title and search.
export default function SiteHeader() {
  const path = window.location.pathname;
  return (
    <header className="bg-white border-b border-gray-200 px-4 sm:px-6 py-2.5 flex flex-wrap items-center gap-x-5 gap-y-2 text-left">
      <a href="/" className="font-bold text-gray-900 whitespace-nowrap">Podcast Network</a>
      <nav className="order-last w-full md:order-none md:w-auto flex gap-1 overflow-x-auto -mx-1 px-1">
        {NAV.map(([href, label, short]) => (
          <a key={href} href={href} aria-current={isActive(href, path) ? 'page' : undefined}
             className={`whitespace-nowrap rounded-md px-2.5 py-1 text-sm ${isActive(href, path)
               ? 'bg-teal-50 text-teal-800 font-medium' : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'}`}>
            {short ? <><span className="sm:hidden">{short}</span><span className="hidden sm:inline">{label}</span></> : label}
          </a>
        ))}
      </nav>
      <div className="flex flex-1 md:flex-none items-center gap-3 ml-auto min-w-0">
        <ProfileSearch className="flex-1 md:w-72" />
        {loggedIn() && <a href="/admin" className="text-xs text-gray-400 hover:text-gray-600 whitespace-nowrap">Admin</a>}
      </div>
    </header>
  );
}

// Header, a scrolling page body and the image credits.
export function SiteShell({ children, width = 'max-w-4xl' }) {
  return (
    <div className="h-screen overflow-y-auto bg-gray-100 font-sans text-left">
      <SiteHeader />
      <main className={`${width} mx-auto p-4 sm:p-6 space-y-5`}>
        {children}
        <Credits className="text-center pt-2" />
      </main>
    </div>
  );
}
