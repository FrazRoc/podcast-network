import { lazy, Suspense } from 'react';
import SiteHeader from './components/SiteHeader';
import PersonPage from './components/PersonPage';
import OrgPage from './components/OrgPage';
import ShowPage from './components/ShowPage';
import PeopleDirectory from './components/PeopleDirectory';
import OrgDirectory from './components/OrgDirectory';
import ShowDirectory from './components/ShowDirectory';
import TopicDirectory from './components/TopicDirectory';
import TopicPage from './components/TopicPage';
import NotFound from './components/NotFound';

// The admin pages load only on /admin: they were most of the bundle every
// visitor downloaded (site review, Oct 2026).
const AdminSuggestions = lazy(() => import('./components/AdminSuggestions'));
const AdminSuggestionsList = lazy(() => import('./components/AdminSuggestionsList'));
const AdminImages = lazy(() => import('./components/AdminImages'));
const AdminPeople = lazy(() => import('./components/AdminPeople'));
const AdminCompanies = lazy(() => import('./components/AdminCompanies'));
const AdminShows = lazy(() => import('./components/AdminShows'));
const AdminEpisodes = lazy(() => import('./components/AdminEpisodes'));
const AdminDuplicates = lazy(() => import('./components/AdminDuplicates'));
const AdminDiagnostics = lazy(() => import('./components/AdminDiagnostics'));
const AdminTopics = lazy(() => import('./components/AdminTopics'));
const AdminLoginGate = lazy(() => import('./components/AdminLoginGate'));
const Stats = lazy(() => import('./components/Stats'));
// The graph library is only needed on the home page.
const PodcastHostNetwork = lazy(() => import('./components/PodcastHostNetwork'));

const Loading = () => <div className="p-6 text-sm text-gray-400">Loading…</div>;
const later = (el) => <Suspense fallback={<Loading />}>{el}</Suspense>;

function App() {
  const path = window.location.pathname;

  if (path === '/stats' || path.startsWith('/stats/'))
    return later(<Stats />);
  // Public directories: /people, /orgs, /shows (a trailing slash too).
  const dir = path.replace(/\/+$/, '');
  if (dir === '/people')
    return <PeopleDirectory />;
  if (dir === '/orgs')
    return <OrgDirectory />;
  if (dir === '/shows')
    return <ShowDirectory />;
  if (dir === '/topics')
    return <TopicDirectory />;
  // Public profile pages: /people/<id>-<slug>, /orgs/<id>-<slug>, /shows/<id>-<slug>.
  if (path.startsWith('/people/'))
    return <PersonPage />;
  if (path.startsWith('/orgs/'))
    return <OrgPage />;
  if (path.startsWith('/shows/'))
    return <ShowPage />;
  if (path.startsWith('/topics/'))
    return <TopicPage />;
  if (path === '/admin/suggestions/list')
    return later(<AdminLoginGate><div className="w-full min-h-screen"><AdminSuggestionsList /></div></AdminLoginGate>);
  if (path === '/admin/images' || path.startsWith('/admin/images/'))
    return later(<AdminLoginGate><div className="w-full min-h-screen"><AdminImages /></div></AdminLoginGate>);
  if (path === '/admin/people' || path.startsWith('/admin/people/'))
    return later(<AdminLoginGate><div className="w-full min-h-screen"><AdminPeople /></div></AdminLoginGate>);
  if (path === '/admin/companies' || path.startsWith('/admin/companies/'))
    return later(<AdminLoginGate><div className="w-full min-h-screen"><AdminCompanies /></div></AdminLoginGate>);
  if (path === '/admin/diagnostics' || path.startsWith('/admin/diagnostics/'))
    return later(<AdminLoginGate><div className="w-full min-h-screen"><AdminDiagnostics /></div></AdminLoginGate>);
  if (path === '/admin/duplicates' || path.startsWith('/admin/duplicates/'))
    return later(<AdminLoginGate><div className="w-full min-h-screen"><AdminDuplicates /></div></AdminLoginGate>);
  if (path === '/admin/shows' || path.startsWith('/admin/shows/'))
    return later(<AdminLoginGate><div className="w-full min-h-screen"><AdminShows /></div></AdminLoginGate>);
  if (path === '/admin/topics' || path.startsWith('/admin/topics/'))
    return later(<AdminLoginGate><div className="w-full min-h-screen"><AdminTopics /></div></AdminLoginGate>);
  if (path === '/admin/episodes' || path.startsWith('/admin/episodes/'))
    return later(<AdminLoginGate><div className="w-full min-h-screen"><AdminEpisodes /></div></AdminLoginGate>);
  if (path === '/admin' || path.startsWith('/admin/'))
    return later(<AdminLoginGate><div className="w-full min-h-screen"><AdminSuggestions /></div></AdminLoginGate>);
  if (dir !== '' && dir !== '/index.html')
    return <NotFound />;
  return (
    <div className="w-full h-screen overflow-hidden flex flex-col">
      <SiteHeader />
      <div className="flex-1 min-h-0">
        {later(<PodcastHostNetwork />)}
      </div>
    </div>
  );
}

export default App;
