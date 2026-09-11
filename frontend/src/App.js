import PodcastHostNetwork from './components/PodcastHostNetwork';
import AdminSuggestions from './components/AdminSuggestions';
import AdminImages from './components/AdminImages';
import AdminPeople from './components/AdminPeople';

function App() {
  const path = window.location.pathname;

  if (path === '/admin/images' || path.startsWith('/admin/images/'))
    return <div className="w-full min-h-screen"><AdminImages /></div>;
  if (path === '/admin/people' || path.startsWith('/admin/people/'))
    return <div className="w-full min-h-screen"><AdminPeople /></div>;
  if (path === '/admin' || path.startsWith('/admin/'))
    return <div className="w-full min-h-screen"><AdminSuggestions /></div>;
  return (
    <div className="w-full h-screen overflow-hidden">
      <PodcastHostNetwork />
    </div>
  );
}

export default App;
