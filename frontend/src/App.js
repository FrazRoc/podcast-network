import PodcastHostNetwork from './components/PodcastHostNetwork';
import AdminSuggestions from './components/AdminSuggestions';
import AdminImages from './components/AdminImages';
import AdminPeople from './components/AdminPeople';
import AdminShows from './components/AdminShows';
import AdminLoginGate from './components/AdminLoginGate';

function App() {
  const path = window.location.pathname;

  if (path === '/admin/images' || path.startsWith('/admin/images/'))
    return <AdminLoginGate><div className="w-full min-h-screen"><AdminImages /></div></AdminLoginGate>;
  if (path === '/admin/people' || path.startsWith('/admin/people/'))
    return <AdminLoginGate><div className="w-full min-h-screen"><AdminPeople /></div></AdminLoginGate>;
  if (path === '/admin/shows' || path.startsWith('/admin/shows/'))
    return <AdminLoginGate><div className="w-full min-h-screen"><AdminShows /></div></AdminLoginGate>;
  if (path === '/admin' || path.startsWith('/admin/'))
    return <AdminLoginGate><div className="w-full min-h-screen"><AdminSuggestions /></div></AdminLoginGate>;
  return (
    <div className="w-full h-screen overflow-hidden">
      <PodcastHostNetwork />
    </div>
  );
}

export default App;
