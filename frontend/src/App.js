import './App.css';
import PodcastHostNetwork from './components/PodcastHostNetwork';
import AdminSuggestions from './components/AdminSuggestions';

function App() {
  const path = window.location.pathname;
  const isAdmin = path === '/admin' || path.startsWith('/admin/');

  return (
    <div className="App w-full h-screen overflow-hidden">
      {isAdmin ? <AdminSuggestions /> : <PodcastHostNetwork />}
    </div>
  );
}

export default App;
