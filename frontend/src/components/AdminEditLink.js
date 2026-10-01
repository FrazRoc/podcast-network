import { getAdminPassword } from '../adminAuth';

// Shown only to a browser that's logged into admin, so a data problem spotted
// on a public page is one click from the screen that fixes it.
const isAdmin = () => { try { return !!getAdminPassword(); } catch { return false; } };

export default function AdminEditLink({ href, children = 'edit in admin', className = '' }) {
  if (!isAdmin()) return null;
  return (
    <a href={href} target="_blank" rel="noopener noreferrer"
       className={`text-[11px] text-gray-400 hover:text-teal-600 hover:underline ${className}`}>
      {children}
    </a>
  );
}
