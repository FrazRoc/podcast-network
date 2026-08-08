import './App.css';
import PodcastHostNetwork from './components/PodcastHostNetwork';
import AdminSuggestions from './components/AdminSuggestions';
import AdminImages from './components/AdminImages';

function App() {
  const path = window.location.pathname;

  if (path === '/admin/images' || path.startsWith('/admin/images/')) {
    return <div className="w-full min-h-screen"><AdminImages /></div>;
  }
  if (path === '/admin' || path.startsWith('/admin/')) {
    return <div className="w-full min-h-screen"><AdminSuggestions /></div>;
  }
  return (
    <div className="App w-full h-screen overflow-hidden">
      <PodcastHostNetwork />
    </div>
  );
}

export default App;
