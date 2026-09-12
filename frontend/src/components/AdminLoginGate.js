import { useState } from 'react';
import { getAdminPassword, setAdminPassword } from '../adminAuth';

export default function AdminLoginGate({ children }) {
  const [password, setPassword] = useState('');
  const [entered, setEntered] = useState(!!getAdminPassword());

  if (entered) return children;

  return (
    <div className="flex items-center justify-center h-screen bg-gray-50">
      <form
        onSubmit={e => {
          e.preventDefault();
          setAdminPassword(password);
          setEntered(true);
        }}
        className="bg-white p-8 rounded-lg shadow-lg w-80"
      >
        <h2 className="text-lg font-semibold mb-4 text-gray-800">Admin Login</h2>
        <input
          type="password"
          value={password}
          onChange={e => setPassword(e.target.value)}
          placeholder="Admin password"
          autoFocus
          className="w-full border border-gray-300 rounded px-3 py-2 mb-4 text-sm focus:outline-none focus:ring-1 focus:ring-teal-500 focus:border-teal-500"
        />
        <button
          type="submit"
          className="w-full bg-teal-600 hover:bg-teal-700 text-white font-medium py-2 rounded"
        >
          Continue
        </button>
      </form>
    </div>
  );
}
