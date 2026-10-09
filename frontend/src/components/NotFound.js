import { useEffect } from 'react';
import { SiteShell } from './SiteHeader';
import { setMeta } from '../profileUtils';

// Any address the app doesn't know. (The static host still answers 200 —
// every path serves index.html — so this is for people, not crawlers.)
export default function NotFound() {
  useEffect(() => setMeta('Page not found · Podcast Network', ''), []);
  return (
    <SiteShell>
      <section className="bg-white rounded-2xl border border-gray-200 p-8 text-center space-y-3">
        <h1 className="text-xl font-semibold text-gray-900">Page not found</h1>
        <p className="text-sm text-gray-500">There's nothing at <span className="font-mono">{window.location.pathname}</span>.</p>
        <p className="text-sm">
          <a href="/" className="text-teal-700 hover:underline">Network</a>
          {' · '}<a href="/people" className="text-teal-700 hover:underline">People</a>
          {' · '}<a href="/shows" className="text-teal-700 hover:underline">Shows</a>
          {' · '}<a href="/topics" className="text-teal-700 hover:underline">Topics</a>
        </p>
      </section>
    </SiteShell>
  );
}
